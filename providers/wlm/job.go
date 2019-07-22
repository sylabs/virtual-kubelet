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
	"bytes"
	"log"

	"golang.org/x/net/context"

	"github.com/sylabs/wlm-operator/pkg/operator/client/clientset/versioned"

	"github.com/pkg/errors"
	"github.com/sylabs/wlm-operator/pkg/operator/apis/wlm/v1alpha1"
	sAPI "github.com/sylabs/wlm-operator/pkg/workload/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	slurmJobKind = "SlurmJob"
	wlmJobKind   = "WlmJob"
)

var (
	errNotWlmJob       = errors.New("pod is not wlm job")
	errUnsupportedKind = errors.New("unsupported kind")
)

// job in an abstraction above pod for more easier controlling wlm jobs lifecycle.
type job struct {
	wlmAPI    sAPI.WorkloadManagerClient
	wlmClient *versioned.Clientset

	jobID      int64
	jobInfo    *sAPI.JobInfo
	jobResults *v1alpha1.JobResults

	wlmPod bool
	pod    v1.Pod
}

// newJob creates new job and checks if the pod is a wlmPod.
func newJob(p v1.Pod, wlmAPI sAPI.WorkloadManagerClient, wlmClient *versioned.Clientset) *job {
	wlmPod := false
	if len(p.OwnerReferences) == 1 {
		k := p.OwnerReferences[0].Kind
		if k == slurmJobKind || k == wlmJobKind {
			wlmPod = true
		}
	}

	return &job{
		wlmAPI:    wlmAPI,
		wlmClient: wlmClient,
		wlmPod:    wlmPod,
		pod:       p,
	}
}

// Start starts wlm or slurm job.
// If pod is not wlmPod returns errNotWlmJob.
func (ji *job) Start() error {
	if !ji.wlmPod {
		return errNotWlmJob
	}

	k := ji.pod.OwnerReferences[0].Kind

	switch k {
	case slurmJobKind:
		return ji.startSlurmJob()
	case wlmJobKind:
		return ji.startWlmJob()
	}

	return errors.Wrap(errUnsupportedKind, k)
}

// Cancel cancels running job.
// If pod is not wlmPod returns errNotWlmJob.
func (ji *job) Cancel() error {
	if !ji.wlmPod {
		return errNotWlmJob
	}

	_, err := ji.wlmAPI.CancelJob(context.Background(), &sAPI.CancelJobRequest{JobId: ji.jobID})
	return errors.Wrapf(err, "can't cancel job %d", ji.jobID)
}

// Logs returns job logs.
// If pod is not wlmPod returns errNotWlmJob.
func (ji *job) Logs() (*bytes.Buffer, error) {
	if !ji.wlmPod {
		return nil, errNotWlmJob
	}

	infoR, err := ji.wlmAPI.JobInfo(context.Background(), &sAPI.JobInfoRequest{JobId: ji.jobID})
	if err != nil && ji.jobInfo == nil {
		return nil, errors.Wrap(err, "can't get wlm job info")
	}

	if infoR != nil {
		ji.jobInfo = infoR.Info[0]
	}

	openResp, err := ji.wlmAPI.OpenFile(context.Background(), &sAPI.OpenFileRequest{Path: ji.jobInfo.StdOut})
	if err != nil {
		return nil, errors.Wrap(err, "can't open wlm job log file")
	}

	buff := &bytes.Buffer{}
	for {
		c, err := openResp.Recv()
		if c != nil {
			_, err := buff.Write(c.Content)
			if err != nil {
				break
			}
		}

		if err != nil {
			break
		}
	}

	return buff, nil
}

// Status returns jobs state.
// If pod is not wlmPod returns errNotWlmJob.
func (ji *job) Status() (sAPI.JobStatus, error) {
	if !ji.wlmPod {
		return 0, errNotWlmJob
	}

	infoR, err := ji.wlmAPI.JobInfo(context.Background(), &sAPI.JobInfoRequest{JobId: ji.jobID})
	if err != nil {
		return 0, errors.Wrapf(err, "can't get status for %d", ji.jobID)
	}
	ji.jobInfo = infoR.Info[0]

	return ji.jobInfo.Status, nil
}

func (ji *job) startSlurmJob() error {
	pod := ji.pod

	sj, err := ji.wlmClient.WlmV1alpha1().
		SlurmJobs(pod.Namespace).
		Get(pod.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "can't get SlurmJob spec")
	}
	ji.jobResults = sj.Spec.Results

	resp, err := ji.wlmAPI.SubmitJob(context.Background(), &sAPI.SubmitJobRequest{
		Partition: partition,
		Script:    sj.Spec.Batch,
	})
	if err != nil {
		return errors.Wrap(err, "can't submit batch script")
	}
	ji.jobID = resp.JobId

	log.Printf("Slurm job %d started", resp.JobId)

	return nil
}

func (ji *job) startWlmJob() error {
	pod := ji.pod
	wj, err := ji.wlmClient.WlmV1alpha1().
		WlmJobs(pod.Namespace).
		Get(pod.OwnerReferences[0].Name, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "can't get WlmJob spec")
	}
	ji.jobResults = wj.Spec.Results
	resp, err := ji.wlmAPI.SubmitJobContainer(context.Background(), &sAPI.SubmitJobContainerRequest{
		ImageName:  wj.Spec.Image,
		Partition:  partition,
		Nodes:      wj.Spec.Resources.Nodes,
		CpuPerNode: wj.Spec.Resources.CpuPerNode,
		MemPerNode: wj.Spec.Resources.MemPerNode,
		WallTime:   wj.Spec.Resources.WallTime,
	})
	if err != nil {
		return errors.Wrap(err, "can't submit job container")
	}

	ji.jobID = resp.JobId

	log.Printf("Wlm job %d started", resp.JobId)

	return nil
}
