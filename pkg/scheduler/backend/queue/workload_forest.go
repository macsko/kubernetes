/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package queue

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// workloadForest maintains a consistent view of observed GenericPodGroup objects (either PodGroup or CompositePodGroup).
// It ensures that scheduling queue invariants are preserved, independent of
// asynchronous updates happening in the scheduler cache.
// Outside of the scheduling queue, cache should be used as the source of truth.
// This structure is not thread-safe and should be accessed only under the lock of the PriorityQueue.
type workloadForest struct {
	podGroups map[fwk.EntityKey]*fwk.GenericPodGroup
	// children maps a parent CompositePodGroup key to its direct children keys (PodGroups or CompositePodGroups).
	// As an invariant of this structure, a child-to-parent relationship is populated here regardless of whether
	// the parent object has been explicitly observed yet. This prevents the need to iterate over all existing
	// groups to retroactively link children when a parent is finally added.
	children                   map[fwk.EntityKey]sets.Set[fwk.EntityKey]
	isCompositePodGroupEnabled bool
}

func newWorkloadForest(isCompositePodGroupEnabled bool) *workloadForest {
	return &workloadForest{
		podGroups:                  make(map[fwk.EntityKey]*fwk.GenericPodGroup),
		children:                   make(map[fwk.EntityKey]sets.Set[fwk.EntityKey]),
		isCompositePodGroupEnabled: isCompositePodGroupEnabled,
	}
}

// addGenericPodGroup adds a GenericPodGroup to the forest.
func (wf *workloadForest) addGenericPodGroup(gpg *fwk.GenericPodGroup) {
	key := gpg.GetKey()
	wf.podGroups[key] = gpg

	if !wf.isCompositePodGroupEnabled {
		return
	}
	parentKey, hasParent := gpg.GetParentKey()
	if !hasParent {
		return
	}

	_, exists := wf.children[parentKey]
	if !exists {
		wf.children[parentKey] = sets.New[fwk.EntityKey]()
	}
	wf.children[parentKey].Insert(key)
}

// updateGenericPodGroup updates a GenericPodGroup in the forest.
func (wf *workloadForest) updateGenericPodGroup(gpg *fwk.GenericPodGroup) {
	wf.podGroups[gpg.GetKey()] = gpg
}

// deleteGenericPodGroup removes a GenericPodGroup from the forest.
func (wf *workloadForest) deleteGenericPodGroup(gpg *fwk.GenericPodGroup) {
	key := gpg.GetKey()
	delete(wf.podGroups, key)

	if !wf.isCompositePodGroupEnabled {
		return
	}
	parentKey, hasParent := gpg.GetParentKey()
	if !hasParent {
		return
	}

	parentChildren, exists := wf.children[parentKey]
	if !exists {
		return
	}
	parentChildren.Delete(key)
	if parentChildren.Len() == 0 {
		delete(wf.children, parentKey)
	}
}

// getRootLookupInfoForPod returns the lookup info of the current root PodGroup or CompositePodGroup for a given pod.
func (wf *workloadForest) getRootLookupInfoForPod(pod *v1.Pod) (*framework.QueuedPodGroupInfo, bool) {
	podGroup, exists := wf.podGroups[podGroupKeyForPod(pod)]
	if !exists {
		return nil, false
	}
	return wf.getRootLookupInfo(podGroup)
}

// getRootLookupInfo returns the lookup info of the current root PodGroup or CompositePodGroup for a given GenericPodGroup.
func (wf *workloadForest) getRootLookupInfo(gpg *fwk.GenericPodGroup) (*framework.QueuedPodGroupInfo, bool) {
	storedGPG, exists := wf.podGroups[gpg.GetKey()]
	if !exists {
		return nil, false
	}

	if !wf.isCompositePodGroupEnabled || !storedGPG.HasParent() {
		return &framework.QueuedPodGroupInfo{
			PodGroupInfo: &framework.PodGroupInfo{
				GenericPodGroup: storedGPG,
			},
		}, true
	}
	return wf.getRootLookupInfoForParentCPG(*storedGPG.GetParentCompositePodGroupName(), storedGPG.GetNamespace())
}

// parentKey returns the parent CompositePodGroup entity key for the given entity, if configured.
func (wf *workloadForest) parentKey(key fwk.EntityKey) (fwk.EntityKey, bool) {
	if !wf.isCompositePodGroupEnabled {
		return fwk.EntityKey{}, false
	}
	if gpg := wf.podGroups[key]; gpg != nil {
		return gpg.GetParentKey()
	}
	return fwk.EntityKey{}, false
}

// traverseAncestors traverses parent pointers upward starting from start.
// It returns the sequence of visited keys, or an error if a cycle or depth exceeding WorkloadMaxTreeDepth is encountered.
func (wf *workloadForest) traverseAncestors(start fwk.EntityKey) ([]fwk.EntityKey, error) {
	path := []fwk.EntityKey{start}
	visited := sets.New(start)

	for {
		if len(path) > schedulingv1alpha3.WorkloadMaxTreeDepth {
			return path, fmt.Errorf("hierarchy depth of %d exceeds maximum allowed depth of %d (path: %s)", len(path), schedulingv1alpha3.WorkloadMaxTreeDepth, formatHierarchyPath(path))
		}
		parent, hasParent := wf.parentKey(path[len(path)-1])
		if !hasParent {
			return path, nil
		}
		path = append(path, parent)
		if visited.Has(parent) {
			return path, fmt.Errorf("cycle detected in composite pod group hierarchy: %s", formatHierarchyPath(path))
		}
		visited.Insert(parent)
	}
}

// traverseSubtree visits all entities in the subtree rooted at root in BFS order.
func (wf *workloadForest) traverseSubtree(root fwk.EntityKey, visitor func(key fwk.EntityKey)) {
	queue := []fwk.EntityKey{root}
	visited := sets.New(root)

	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]

		visitor(curr)

		for child := range wf.children[curr] {
			if !visited.Has(child) {
				visited.Insert(child)
				queue = append(queue, child)
			}
		}
	}
}

// collectSubtree collects all reachable descendants (including seeds) in the subtree(s) rooted at seeds.
func (wf *workloadForest) collectSubtree(seeds ...fwk.EntityKey) sets.Set[fwk.EntityKey] {
	result := sets.New[fwk.EntityKey]()
	for _, seed := range seeds {
		wf.traverseSubtree(seed, func(key fwk.EntityKey) {
			result.Insert(key)
		})
	}
	return result
}

// getRootLookupInfoForParentCPG is a helper to traverse up the parent chain and return the lookup info of the root CompositePodGroup.
// It should be called only when the CompositePodGroup feature gate is enabled.
func (wf *workloadForest) getRootLookupInfoForParentCPG(parentName, namespace string) (*framework.QueuedPodGroupInfo, bool) {
	cpgKey := fwk.CompositePodGroupKey(namespace, parentName)
	path, err := wf.traverseAncestors(cpgKey)
	if err != nil {
		// TODO(jdzikowski): propagate logger to the getPod method in the scheduling queue.
		utilruntime.HandleError(err)
		return nil, false
	}
	rootKey := path[len(path)-1]
	gpg, exists := wf.podGroups[rootKey]
	if !exists || gpg.HasParent() {
		return nil, false
	}
	return newCompositePodGroupInfoForLookup(gpg.GetNamespace(), gpg.GetName()), true
}

// getLeafPodGroups returns all PodGroups that are leaf nodes in the subtree rooted at the given rootLookupInfo.
func (wf *workloadForest) getLeafPodGroups(logger klog.Logger, rootLookupInfo *framework.QueuedPodGroupInfo) []*schedulingv1beta1.PodGroup {
	rootKey := rootLookupInfo.GetKey()
	if rootLookupInfo.GetType() == fwk.PodGroupKeyType {
		if gpg, exists := wf.podGroups[rootKey]; exists && gpg.PodGroup != nil {
			return []*schedulingv1beta1.PodGroup{gpg.PodGroup}
		}
		return nil
	}

	var pgs []*schedulingv1beta1.PodGroup
	wf.traverseSubtree(rootKey, func(key fwk.EntityKey) {
		if gpg, isPG := wf.podGroups[key]; isPG && gpg.PodGroup != nil {
			pgs = append(pgs, gpg.PodGroup)
		}
	})
	return pgs
}

// buildPodGroupInfo recursively constructs a PodGroupInfo representation for a given GenericPodGroup
// and all its children, using the provided visited set to detect cycles in the hierarchy.
func (wf *workloadForest) buildPodGroupInfo(logger klog.Logger, gpg *fwk.GenericPodGroup, visited sets.Set[fwk.EntityKey]) *framework.PodGroupInfo {
	key := gpg.GetKey()
	if visited.Has(key) {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cycle detected in composite pod group hierarchy when building PodGroupInfo", "groupType", gpg.GetType(), "group", klog.KObj(gpg))
		return nil
	}
	visited.Insert(key)

	pgi := &framework.PodGroupInfo{
		GenericPodGroup: gpg,
		Children:        make([]*framework.PodGroupInfo, 0),
	}

	childrenSet, ok := wf.children[key]
	if !ok {
		return pgi
	}
	for childKey := range childrenSet {
		if childGPG, ok := wf.podGroups[childKey]; ok {
			if childInfo := wf.buildPodGroupInfo(logger, childGPG, visited); childInfo != nil {
				pgi.Children = append(pgi.Children, childInfo)
			}
		}
	}
	return pgi
}

// buildQueuedPodGroupInfo constructs a QueuedPodGroupInfo starting from the provided root lookup info,
// building out the full hierarchy of PodGroupInfo nodes and initializing the QueuedPodInfos map.
func (wf *workloadForest) buildQueuedPodGroupInfo(logger klog.Logger, rootLookup *framework.QueuedPodGroupInfo) *framework.QueuedPodGroupInfo {
	if rootLookup == nil {
		return nil
	}
	key := rootLookup.GetKey()
	gpg, ok := wf.podGroups[key]
	if !ok {
		return nil
	}
	return &framework.QueuedPodGroupInfo{
		PodGroupInfo:   wf.buildPodGroupInfo(logger, gpg, sets.New[fwk.EntityKey]()),
		QueuedPodInfos: make(map[fwk.EntityKey][]*framework.QueuedPodInfo),
	}
}

func formatHierarchyPath(path []fwk.EntityKey) string {
	parts := make([]string, len(path))
	for i, k := range path {
		parts[i] = k.String()
	}
	return strings.Join(parts, " -> ")
}
