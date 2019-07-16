package wlm

import (
	"bytes"
	"log"
	"time"

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

type job struct {
	wlmApi    sAPI.WorkloadManagerClient
	wlmClient *versioned.Clientset

	jobID      int64
	jobInfo    *sAPI.JobInfo
	jobResults *v1alpha1.JobResults

	wlmPod bool
	pod    v1.Pod
}

func newJob(p v1.Pod, wlmApi sAPI.WorkloadManagerClient, wlmClient *versioned.Clientset) *job {
	wlmPod := false
	if len(p.OwnerReferences) == 1 {
		k := p.OwnerReferences[0].Kind
		if k == slurmJobKind || k == wlmJobKind {
			wlmPod = true
		}
	}

	return &job{
		wlmApi:    wlmApi,
		wlmClient: wlmClient,
		wlmPod:    wlmPod,
		pod:       p,
	}
}

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

func (ji *job) UpdatePod(p v1.Pod) {
	ji.pod = p
}

func (ji *job) Cancel() error {
	if !ji.wlmPod {
		return errNotWlmJob
	}

	_, err := ji.wlmApi.CancelJob(context.Background(), &sAPI.CancelJobRequest{JobId: ji.jobID})
	return errors.Wrapf(err, "can't cancel job %d", ji.jobID)
}

func (ji *job) Logs() (*bytes.Buffer, error) {
	if !ji.wlmPod {
		return nil, errNotWlmJob
	}

	infoR, err := ji.wlmApi.JobInfo(context.Background(), &sAPI.JobInfoRequest{JobId: ji.jobID})
	if err != nil && ji.jobInfo == nil {
		return nil, errors.Wrap(err, "can't get wlm job info")
	}

	if infoR != nil {
		ji.jobInfo = infoR.Info[0]
	}

	openResp, err := ji.wlmApi.OpenFile(context.Background(), &sAPI.OpenFileRequest{Path: ji.jobInfo.StdOut})
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

func (ji *job) Status() (sAPI.JobStatus, error) {
	if !ji.wlmPod {
		return 0, errNotWlmJob
	}

	infoR, err := ji.wlmApi.JobInfo(context.Background(), &sAPI.JobInfoRequest{JobId: ji.jobID})
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

	resp, err := ji.wlmApi.SubmitJob(context.Background(), &sAPI.SubmitJobRequest{
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
	resp, err := ji.wlmApi.SubmitJobContainer(context.Background(), &sAPI.SubmitJobContainerRequest{
		ImageName:  wj.Spec.Image,
		Partition:  partition,
		Nodes:      wj.Spec.Resources.Nodes,
		CpuPerNode: wj.Spec.Resources.CpuPerNode,
		MemPerNode: wj.Spec.Resources.MemPerNode,
		WallTime:   int64(wj.Spec.Resources.WallTime * time.Second),
	})
	if err != nil {
		return errors.Wrap(err, "can't submit job container")
	}

	ji.jobID = resp.JobId

	log.Printf("Wlm job %d started", resp.JobId)

	return nil
}
