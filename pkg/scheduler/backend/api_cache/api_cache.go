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

package apicache

import (
	"context"

	v1 "k8s.io/api/core/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/api_calls"
)

// APICache is responsible for sending API calls' requests through scheduling queue or cache.
type APICache struct {
	// apiDispatcher is used for the methods that are expected to send API calls.
	// It's non-nil only if the SchedulerAsyncAPICalls feature gate is enabled.
	apiDispatcher fwk.APIDispatcher
}

func New(apiDispatcher fwk.APIDispatcher) *APICache {
	return &APICache{
		apiDispatcher: apiDispatcher,
	}
}

// PatchPodStatus handles the pod status update by sending an update API call and changing the pod in the queue accordingly.
func (c *APICache) PatchPodStatus(pod *v1.Pod, condition *v1.PodCondition, nominatingInfo *framework.NominatingInfo) error {
	err := c.apiDispatcher.Add(apicalls.Implementations.PodStatusPatch(pod, condition, nominatingInfo), fwk.APICallOptions{})
	if fwk.IsUnexpectedError(err) {
		return err
	}
	return nil
}

// BindPod handles the pod binding by adding a bind API call to the dispatcher.
// It returns a channel that can be used to wait for the call's completion.
func (c *APICache) BindPod(binding *v1.Binding) (<-chan error, error) {
	// Don't store anything in the cache, as the pod is already assumed, and in case of a binding failure, it will be forgotten.
	onFinish := make(chan error, 1)
	err := c.apiDispatcher.Add(apicalls.Implementations.PodBinding(binding), fwk.APICallOptions{
		OnFinish: onFinish,
	})
	if fwk.IsUnexpectedError(err) {
		return onFinish, err
	}
	return onFinish, nil
}

// PreemptPod handles the pod preemption by adding a preemption API call to the dispatcher and changing the pod in the cache accordingly.
// It returns a channel that can be used to wait for the call's completion.
func (c *APICache) PreemptPod(victim *v1.Pod, preemptor *v1.Pod, condition *v1.PodCondition) (<-chan error, error) {
	// Don't store anything in the cache, as we don't know how long will the preemption take
	// and it's more reliable to wait for the result on the event handler.
	onFinish := make(chan error, 1)
	err := c.apiDispatcher.Add(apicalls.Implementations.PodPreemption(victim, preemptor, condition), fwk.APICallOptions{
		OnFinish: onFinish,
	})
	if fwk.IsUnexpectedError(err) {
		return onFinish, err
	}
	return onFinish, nil
}

// WaitOnFinish blocks until the result of an API call is sent to the given onFinish channel
// (returned by methods BindPod or PreemptPod).
//
// It returns the error received from the channel.
// It also returns nil if the call was skipped or overwritten,
// as these are considered successful lifecycle outcomes.
func (c *APICache) WaitOnFinish(ctx context.Context, onFinish <-chan error) error {
	select {
	case err := <-onFinish:
		if fwk.IsUnexpectedError(err) {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
