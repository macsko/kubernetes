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

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

type podGroupCache struct {
	// TODO(CompositePodGroup): Store CPGs as well. Construct a hierarchy tree?
	podGroups map[string]*schedulingv1alpha3.PodGroup
}

func newPodGroupCache() *podGroupCache {
	return &podGroupCache{ // TODO: Unit tests
		podGroups: make(map[string]*schedulingv1alpha3.PodGroup),
	}
}

func (c *podGroupCache) addOrUpdate(pg *schedulingv1alpha3.PodGroup) {
	c.podGroups[podGroupKey(pg)] = pg
}

func (c *podGroupCache) delete(pg *schedulingv1alpha3.PodGroup) {
	delete(c.podGroups, podGroupKey(pg))
}

func (c *podGroupCache) get(pgKey string) (*schedulingv1alpha3.PodGroup, bool) {
	pg, ok := c.podGroups[pgKey]
	return pg, ok
}

func (c *podGroupCache) getForPod(pod *v1.Pod) (*schedulingv1alpha3.PodGroup, bool) {
	return c.get(podGroupKeyForPod(pod))
}

func (c *podGroupCache) getForGroupInfo(pgInfo *framework.QueuedPodGroupInfo) (*schedulingv1alpha3.PodGroup, bool) {
	return c.get(podGroupKeyForGroupInfo(pgInfo))
}

func podGroupKey(podGroup *schedulingv1alpha3.PodGroup) string {
	return fmt.Sprintf("%s/%s", podGroup.Namespace, podGroup.Name)
}

func podGroupKeyForPod(pod *v1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
}

func podGroupKeyForGroupInfo(pgInfo *framework.QueuedPodGroupInfo) string {
	return fmt.Sprintf("%s/%s", pgInfo.Namespace, pgInfo.Name)
}
