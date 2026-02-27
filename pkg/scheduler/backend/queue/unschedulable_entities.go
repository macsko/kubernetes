/*
Copyright 2025 The Kubernetes Authors.

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
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// unschedulableEntities holds pods and pod groups that cannot be scheduled.
type unschedulableEntities struct {
	// // podInfoMap is a map keyed by a pod's key and the value is the QueuedPodInfo.
	// podInfoMap map[string]*framework.QueuedPodInfo
	// // podGroupInfoMap is a map keyed by a pod group's key and the value is the QueuedPodGroupInfo.
	// podGroupInfoMap map[string]*framework.QueuedPodGroupInfo
	// entityInfoMap is a map keyed by an entity's key and the value is the QueuedEntityInfo.
	entityInfoMap map[string]framework.QueuedEntityInfo
	keyFunc       func(framework.QueuedEntityInfo) string
	// unschedulableRecorder and gatedRecorder track the number of entities in the unschedulable queue.
	// unschedulableRecorder tracks standard unschedulable entities, while gatedRecorder tracks entities
	// that are specifically blocked by scheduling gates.
	unschedulableRecorder, gatedRecorder metrics.MetricRecorder
}

// newUnschedulableEntities initializes a new object of unschedulableEntities.
func newUnschedulableEntities(keyFunc func(framework.QueuedEntityInfo) string, unschedulableRecorder, gatedRecorder metrics.MetricRecorder) *unschedulableEntities {
	return &unschedulableEntities{
		entityInfoMap:         make(map[string]framework.QueuedEntityInfo),
		keyFunc:               queuedEntityKeyFunc,
		unschedulableRecorder: unschedulableRecorder,
		gatedRecorder:         gatedRecorder,
	}
}

// updateMetricsOnStateChange handles the metric accounting when an entity changes
// between Gated and Unschedulable states.
func (u *unschedulableEntities) updateMetricsOnStateChange(gatedBefore, isGated bool) {
	if gatedBefore == isGated {
		return
	}

	if gatedBefore {
		// Transition: Gated -> Ungated
		u.gatedRecorder.Dec()
		u.unschedulableRecorder.Inc()
	} else {
		// Transition: Ungated -> Gated
		u.gatedRecorder.Inc()
		u.unschedulableRecorder.Dec()
	}
}

// addOrUpdate adds an entity to the unschedulable entityInfoMap.
// The event should show which event triggered the addition and is used for the metric recording.
func (u *unschedulableEntities) addOrUpdate(entity framework.QueuedEntityInfo, gatedBefore bool, event string) {
	entityKey := u.keyFunc(entity)
	if _, exists := u.entityInfoMap[entityKey]; exists {
		u.updateMetricsOnStateChange(gatedBefore, entity.Gated())
	} else {
		if entity.Gated() {
			u.gatedRecorder.Inc() // TODO: Metrics
		} else {
			u.unschedulableRecorder.Inc()
		}
		if metrics.SchedulerQueueIncomingPods != nil {
			metrics.SchedulerQueueIncomingPods.WithLabelValues("unschedulable", event).Inc()
		}
	}
	u.entityInfoMap[entityKey] = entity
}

// delete deletes an entity from the unschedulable entityInfoMap.
// The `gated` parameter is used to figure out which metric should be decreased.
func (u *unschedulableEntities) delete(entity framework.QueuedEntityInfo, gated bool) {
	entityKey := u.keyFunc(entity)
	if _, exists := u.entityInfoMap[entityKey]; exists {
		if gated {
			u.gatedRecorder.Dec()
		} else {
			u.unschedulableRecorder.Dec()
		}
	}
	delete(u.entityInfoMap, entityKey)
}

// get returns the QueuedEntityInfo if an entity with the same key is found in the map.
// It returns nil otherwise.
func (u *unschedulableEntities) get(entity framework.QueuedEntityInfo) framework.QueuedEntityInfo {
	entityKey := u.keyFunc(entity)
	if entity, exists := u.entityInfoMap[entityKey]; exists {
		return entity
	}
	return nil
}

// clear removes all the entries from the unschedulable entityInfoMap.
func (u *unschedulableEntities) clear() {
	u.entityInfoMap = make(map[string]framework.QueuedEntityInfo)
	u.unschedulableRecorder.Clear()
	u.gatedRecorder.Clear()
}
