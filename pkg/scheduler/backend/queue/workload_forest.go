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
	"slices"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
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

// getRootLookupInfoForParentCPG is a helper to traverse up the parent chain and return the lookup info of the root CompositePodGroup.
// It should be called only when the CompositePodGroup feature gate is enabled.
func (wf *workloadForest) getRootLookupInfoForParentCPG(parentName, namespace string) (*framework.QueuedPodGroupInfo, bool) {
	currParentName := parentName
	visited := sets.New[fwk.EntityKey]()
	for {
		cpgKey := fwk.CompositePodGroupKey(namespace, currParentName)
		if visited.Has(cpgKey) {
			// TODO(jdzikowski): propagate logger to the getPod method in the scheduling queue.
			utilruntime.HandleError(fmt.Errorf("cycle detected in composite pod group hierarchy when getting root info: %s/%s", parentName, namespace))
			return nil, false
		}
		visited.Insert(cpgKey)

		cpg, exists := wf.podGroups[cpgKey]
		if !exists {
			return nil, false
		}

		if !cpg.HasParent() {
			return newCompositePodGroupInfoForLookup(cpg.GetNamespace(), cpg.GetName()), true
		}
		currParentName = *cpg.GetParentCompositePodGroupName()
	}
}

// getLeafPodGroups returns all PodGroups that are leaf nodes in the subtree rooted at the given rootLookupInfo.
func (wf *workloadForest) getLeafPodGroups(logger klog.Logger, rootLookupInfo *framework.QueuedPodGroupInfo) []*schedulingv1beta1.PodGroup {
	key := rootLookupInfo.GetKey()
	if rootLookupInfo.GetType() == fwk.PodGroupKeyType {
		gpg, exists := wf.podGroups[key]
		if !exists {
			return nil
		}
		return []*schedulingv1beta1.PodGroup{gpg.PodGroup}
	}

	var pgs []*schedulingv1beta1.PodGroup
	queue := []fwk.EntityKey{key}
	visited := sets.New[fwk.EntityKey]()

	for len(queue) > 0 {
		currKey := queue[0]
		queue = queue[1:]

		if visited.Has(currKey) {
			utilruntime.HandleErrorWithLogger(logger, nil, "Cycle detected in composite pod group hierarchy when getting leaf PodGroups", "compositePodGroup", klog.KObj(rootLookupInfo))
			return pgs
		}
		visited.Insert(currKey)

		children, exists := wf.children[currKey]
		if !exists {
			continue
		}

		for childKey := range children {
			gpg, ok := wf.podGroups[childKey]
			if !ok {
				continue
			}
			if gpg.PodGroup != nil {
				pgs = append(pgs, gpg.PodGroup)
			} else if gpg.CompositePodGroup != nil {
				queue = append(queue, childKey)
			}
		}
	}

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

// validateHierarchy validates that the hierarchy containing key (PodGroup) has no cycles,
// does not exceed WorkloadMaxTreeDepth in any of its branches, and that all the ancestors of key
// exist in the forest.
// It returns all observed groups belonging to that hierarchy: the ancestor path walked up from key,
// plus everything reachable below those ancestors. The whole hierarchy is returned even on failure,
// because an invalid hierarchy makes all of its groups unschedulable, and the caller reports
// the problem on each of them.
func (wf *workloadForest) validateHierarchy(key fwk.EntityKey) ([]*fwk.GenericPodGroup, error) {
	visited := sets.New[fwk.EntityKey]()
	// The ancestors are kept in order, so that a cycle can be extracted as a suffix of the path.
	var path []fwk.EntityKey
	currentKey := key

	for {
		if visited.Has(currentKey) {
			hierarchy, _ := wf.collectSubtrees(sets.New(path...), false)
			return hierarchy, util.NewHierarchyCycleError(minEntityKey(path[slices.Index(path, currentKey):]))
		}
		visited.Insert(currentKey)
		// The key is recorded before the lookup below, so that the observed children of a missing
		// group are still collected as part of the hierarchy.
		path = append(path, currentKey)

		gpg, ok := wf.podGroups[currentKey]
		if !ok {
			hierarchy, _ := wf.collectSubtrees(sets.New(path...), false)
			return hierarchy, util.NewHierarchyGroupNotFoundError(currentKey)
		}

		if !wf.isCompositePodGroupEnabled || !gpg.HasParent() {
			break
		}
		parentKey, _ := gpg.GetParentKey()
		currentKey = parentKey
	}

	// currentKey is the root of the hierarchy. The depth of the branches other than the one walked
	// above is only known when walking back down, so the whole hierarchy is verified there.
	return wf.collectSubtrees(sets.New(currentKey), true)
}

// collectSubtrees returns the given groups together with all their descendants, skipping the groups
// that have not been observed yet. With checkDepth set, it also verifies that no branch of the
// collected subtrees exceeds WorkloadMaxTreeDepth, counting the given roots as the first level.
// Cycles are not detected here: a group has a single parent, so the groups forming a cycle are only
// reachable by walking into it, never downwards from a group outside of it.
func (wf *workloadForest) collectSubtrees(roots sets.Set[fwk.EntityKey], checkDepth bool) ([]*fwk.GenericPodGroup, error) {
	type groupAtDepth struct {
		key   fwk.EntityKey
		depth int
	}
	visited := roots.Clone()
	queue := make([]groupAtDepth, 0, len(roots))
	for key := range roots {
		queue = append(queue, groupAtDepth{key: key, depth: 1})
	}
	hierarchy := make([]*fwk.GenericPodGroup, 0, len(queue))
	var tooDeep *groupAtDepth

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		if gpg, ok := wf.podGroups[current.key]; ok {
			hierarchy = append(hierarchy, gpg)
		}
		// The traversal continues past a too deep group, because the caller reports the problem
		// on every group of the hierarchy.
		if checkDepth && current.depth > schedulingv1alpha3.WorkloadMaxTreeDepth &&
			(tooDeep == nil || current.key.String() < tooDeep.key.String()) {
			tooDeep = &current
		}
		for childKey := range wf.children[current.key] {
			if !visited.Has(childKey) {
				visited.Insert(childKey)
				queue = append(queue, groupAtDepth{key: childKey, depth: current.depth + 1})
			}
		}
	}

	if tooDeep != nil {
		return hierarchy, util.NewHierarchyDepthExceededError(tooDeep.depth, tooDeep.key)
	}
	return hierarchy, nil
}

// minEntityKey returns the smallest of the given keys, which must not be empty.
// When several groups break the same rule, the error names only one of them, and picking it
// deterministically keeps the reported message - and the conditions patched from it - stable.
func minEntityKey(keys []fwk.EntityKey) fwk.EntityKey {
	res := keys[0]
	for _, key := range keys[1:] {
		if key.String() < res.String() {
			res = key
		}
	}
	return res
}
