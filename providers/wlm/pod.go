// Copyright (c) 2019 Sylabs, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package wlm

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sylabs/wlm-operator/pkg/operator/apis/wlm/v1alpha1"
	sAPI "github.com/sylabs/wlm-operator/pkg/workload/api"
	"github.com/virtual-kubelet/virtual-kubelet/vkubelet/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CreatePod created new pod.
// If a pod owner is a SlurmJob, it will start job on a Slurm cluster,
// other pods will not be launched.
func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Create Pod %s", podName(pod.Namespace, pod.Name))

	now := metav1.NewTime(time.Now())
	pod.Status.StartTime = &now

	job := newJob(*pod, p.wlmAPI, p.wlmClient)

	if err := job.Start(); err != nil && err != errNotWlmJob {
		return errors.Wrap(err, "Could not start job")
	}

	p.pods[podName(pod.Namespace, pod.Name)] = job

	return nil
}

// UpdatePod updates pod.
func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Update pod %s", podName(pod.Namespace, pod.Name))
	pi, ok := p.pods[podName(pod.Namespace, pod.Name)]
	if !ok {
		return ErrPodNotFound
	}
	pi.pod = *pod

	return nil
}

// DeletePod deletes pod.
func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Delete %s", podName(pod.Namespace, pod.Name))
	pi := p.pods[podName(pod.Namespace, pod.Name)]
	if err := pi.Cancel(); err != nil && err != errNotWlmJob {
		return errors.Wrap(err, "Could not cancel job")
	}

	delete(p.pods, podName(pod.Namespace, pod.Name))
	return nil
}

// GetPod returns a pod.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	log.Printf("Get Pod %s", podName(namespace, name))
	pj, ok := p.pods[podName(namespace, name)]
	if !ok {
		return nil, ErrPodNotFound
	}

	return &pj.pod, nil
}

// GetContainerLogs returns logs if requested pod is SlurmJob,
// otherwise returns empty reader.
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, pName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	log.Printf("GetContainerLogs n:%s pod:%s containerName:%s", namespace, pName, containerName)

	pi, ok := p.pods[podName(namespace, pName)]
	if !ok {
		return nil, ErrPodNotFound
	}

	logs, err := pi.Logs()
	if err != nil {
		if err != errNotWlmJob {
			return nil, errors.Wrap(err, "Could not get logs")
		}

		return ioutil.NopCloser(strings.NewReader("")), nil
	}

	return ioutil.NopCloser(logs), nil
}

// RunInContainer wlm doesn't support it.
func (p *Provider) RunInContainer(ctx context.Context, namespace, name, container string, cmd []string, attach api.AttachIO) error {
	return ErrNotSupported
}

// GetPodStatus retrieves the status of a given pod by name.
func (p *Provider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	log.Printf("Get Pod Status namespace:%s name:%s", namespace, name)

	status := &v1.PodStatus{
		Phase:  v1.PodRunning,
		HostIP: "1.2.3.4",
		PodIP:  "5.6.7.8",
		Conditions: []v1.PodCondition{
			{
				Type:   v1.PodInitialized,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodReady,
				Status: v1.ConditionTrue,
			},
			{
				Type:   v1.PodScheduled,
				Status: v1.ConditionTrue,
			},
		},
	}

	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	now := metav1.NewTime(time.Now())
	if pod.Status.StartTime != nil {
		now = *pod.Status.StartTime
	}

	for _, container := range pod.Spec.Containers {
		status.ContainerStatuses = append(status.ContainerStatuses, v1.ContainerStatus{
			Name:         container.Name,
			Image:        container.Image,
			Ready:        true,
			RestartCount: 0,
			State: v1.ContainerState{
				Running: &v1.ContainerStateRunning{
					StartedAt: now,
				},
			},
		})
	}

	pj, ok := p.pods[podName(namespace, name)]
	if ok {
		jobStatus, err := pj.Status()
		if err != nil {
			if err != errNotWlmJob {
				return nil, errors.Wrap(err, "Could not get job status")
			}

			return status, nil
		}

		switch jobStatus {
		case sAPI.JobStatus_COMPLETED:
			status.ContainerStatuses[0].State.Terminated = &v1.ContainerStateTerminated{
				Reason: "Job finished",
			}
			status.Phase = v1.PodSucceeded
			if pj.jobResults != nil {
				if err := p.startCollectingResultsPod(&pj.pod, pj.jobResults); err != nil {
					log.Printf("Can't collect job results: %s", err)
				}
			}
		case sAPI.JobStatus_FAILED, sAPI.JobStatus_CANCELLED:
			status.Phase = v1.PodFailed
		}
	}

	return status, nil
}

// GetPods retrieves a list of all pods scheduled to run.
func (p *Provider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	log.Println("Get Pods")

	pods := make([]*v1.Pod, 0, len(p.pods))
	for _, pj := range p.pods {
		pods = append(pods, &pj.pod)
	}

	return pods, nil
}

// startCollectingResultsPod creates a new pod which will transfer data from
// slurm cluster to mounted volume.
func (p *Provider) startCollectingResultsPod(jobPod *v1.Pod, r *v1alpha1.JobResults) error {
	collectPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobPod.Name + "-collect",
			Namespace: jobPod.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "cr1",
					Image:           resultsImage,
					ImagePullPolicy: v1.PullAlways,
					Args: []string{
						fmt.Sprintf("--from=%s", r.From),
						"--to=/collect",
						"--sock=/red-box.sock",
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "red-box-sock",
							MountPath: "/red-box.sock",
						},
						{
							Name:      r.Mount.Name,
							MountPath: "/collect",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "red-box-sock",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/var/run/syslurm/red-box.sock",
							Type: &[]v1.HostPathType{v1.HostPathSocket}[0],
						},
					},
				},
				r.Mount,
			},
			NodeSelector: map[string]string{
				"kubernetes.io/hostname": vkHostNode,
			},
			SecurityContext: &v1.PodSecurityContext{
				RunAsUser:  &p.uid,
				RunAsGroup: &p.gid,
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	collectPod.OwnerReferences = jobPod.OwnerReferences // allows k8s to delete pod after parent SlurmJob kind be deleted.

	_, err := p.coreClient.Pods(jobPod.Namespace).Create(collectPod)
	return errors.Wrap(err, "could not create collect results pod")
}

func podName(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}
