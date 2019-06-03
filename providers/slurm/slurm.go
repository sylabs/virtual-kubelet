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

package slurm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/sylabs/slurm-operator/pkg/operator/apis/slurm/v1alpha1"

	"github.com/pkg/errors"

	"github.com/sylabs/slurm-operator/pkg/operator/client/clientset/versioned"
	sAPI "github.com/sylabs/slurm-operator/pkg/workload/api"
	"github.com/virtual-kubelet/virtual-kubelet/vkubelet/api"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	stats "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
)

const (
	slurmJobKind = "SlurmJob"
)

var (
	vkHostNode = os.Getenv("VK_HOST_NAME")
	vkPodName  = os.Getenv("VK_POD_NAME")
	partition  = os.Getenv("PARTITION")
	redBoxSock = os.Getenv("RED_BOX_SOCK")

	ErrNotSupported = errors.New("not supported")
)

type podInfo struct {
	jobID   int64
	jobInfo *sAPI.JobInfo
	jobSpec *v1alpha1.SlurmJobSpec

	pod *v1.Pod
}

// Provider implements the virtual-kubelet provider interface by forwarding kubelet calls to a slurm cluster.
type Provider struct {
	startTime time.Time

	nodeName           string
	operatingSystem    string
	endpoint           *url.URL
	daemonEndpointPort int32
	internalIP         string

	slurmAPI  sAPI.WorkloadManagerClient
	slurmJobC *versioned.Clientset

	coreC *corev1.CoreV1Client

	pods map[string]*podInfo
}

// NewProvider creates a new SlurmProvider.
// Start watch dog for updating nodes resources.
func NewProvider(nodeName, operatingSystem, internalIP string, daemonEndpointPort int32) (*Provider, error) {
	conn, err := grpc.Dial("unix://"+redBoxSock, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("can't connect to %s %s", redBoxSock, err)
	}
	redBoxClient := sAPI.NewWorkloadManagerClient(conn)

	// gettings k8s config.
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("can't fetch cluster config: %v", err)
	}

	// corev1 client set is required to create collecting results pod.
	coreC, err := corev1.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("could not create core client: %v", err)
	}

	nodePatcher, err := newNodePatcher(coreC)
	if err != nil {
		return nil, errors.Wrap(err, "can't create nodePatcher")
	}

	// start updating nodes labels (cpu per node, mem per node, nodes, features).
	go newWatchDog(nodePatcher, redBoxClient, partition).watch()

	// SlurmJob ClientSet.
	slurmC, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("can't create slurm client set")
	}

	provider := &Provider{
		startTime: time.Now(),

		nodeName:           nodeName,
		operatingSystem:    operatingSystem,
		internalIP:         internalIP,
		daemonEndpointPort: daemonEndpointPort,
		slurmAPI:           redBoxClient,
		slurmJobC:          slurmC,
		coreC:              coreC,
		pods:               make(map[string]*podInfo),
	}

	return provider, nil
}

// CreatePod stores pod.
// if pod owner is SlurmJob it starts job on Slurm cluster,
// other pods will not be launched.
func (p *Provider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Create Pod %s", podName(pod.Namespace, pod.Name))

	var jobID int64
	var jobSpec *v1alpha1.SlurmJobSpec

	if len(pod.OwnerReferences) == 1 && pod.OwnerReferences[0].Kind == slurmJobKind {
		sj, err := p.slurmJobC.SlurmV1alpha1().
			SlurmJobs(pod.Namespace).
			Get(pod.OwnerReferences[0].Name, metav1.GetOptions{})
		if err != nil {
			return errors.Wrap(err, "can't get slurm job")
		}

		jobSpec = &sj.Spec

		resp, err := p.slurmAPI.SubmitJob(ctx, &sAPI.SubmitJobRequest{
			Partition: partition,
			Script:    sj.Spec.Batch,
		})
		if err != nil {
			return errors.Wrap(err, "can't submit batch script")
		}

		jobID = resp.JobId

		log.Printf("Job %d started", resp.JobId)
	}

	now := metav1.NewTime(time.Now())
	pod.Status.StartTime = &now

	p.pods[podName(pod.Namespace, pod.Name)] = &podInfo{
		jobID:   jobID,
		jobSpec: jobSpec,
		pod:     pod,
	}

	return nil
}

// UpdatePod updates pod.
func (p *Provider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Update pod %s", podName(pod.Namespace, pod.Name))
	pi, ok := p.pods[podName(pod.Namespace, pod.Name)]
	if !ok {
		return errors.New("there is no requested pod")
	}
	pi.pod = pod

	return nil
}

// DeletePod deletes pod.
func (p *Provider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Delete %s", podName(pod.Namespace, pod.Name))
	delete(p.pods, podName(pod.Namespace, pod.Name))
	return nil
}

// GetPod returns a pod.
func (p *Provider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	log.Printf("Get Pod %s", podName(namespace, name))
	pj, ok := p.pods[podName(namespace, name)]
	if !ok {
		return nil, errors.New("there is no requested pod")
	}

	return pj.pod, nil
}

// GetContainerLogs returns logs if requeted pod is SlurmJob
// otherwise returns empty reader.
func (p *Provider) GetContainerLogs(ctx context.Context, namespace, pName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	log.Printf("GetContainerLogs n:%s pod:%s containerName:%s", namespace, pName, containerName)

	pi, ok := p.pods[podName(namespace, pName)]
	if !ok {
		return nil, errors.New("there is no requested pod")
	}

	if pi.jobID == 0 { //skipping not slurm jobs.
		return ioutil.NopCloser(strings.NewReader("")), nil
	}

	infoR, err := p.slurmAPI.JobInfo(ctx, &sAPI.JobInfoRequest{JobId: pi.jobID})
	if err != nil && pi.jobInfo == nil {
		return nil, errors.Wrap(err, "can't get slurm job info")
	}

	if infoR != nil {
		pi.jobInfo = infoR.Info[0]
	}

	openResp, err := p.slurmAPI.OpenFile(context.Background(), &sAPI.OpenFileRequest{Path: pi.jobInfo.StdOut})
	if err != nil {
		return nil, errors.Wrap(err, "can't open slurm job log file")
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

	return ioutil.NopCloser(buff), nil
}

// Get full pod name as defined in the provider context.
func (p *Provider) GetPodFullName(namespace string, pod string) string {
	log.Printf("GetPodFullName n:%s p:%s", namespace, pod)
	return ""
}

// RunInContainer SLURM doesn't support it.
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
	if ok && pj.jobID != 0 { // we need only Slurm jobs.
		infoR, err := p.slurmAPI.JobInfo(ctx, &sAPI.JobInfoRequest{JobId: pj.jobID})
		if err != nil {
			return nil, errors.Wrapf(err, "can't get status for %d", pj.jobID)
		}
		pj.jobInfo = infoR.Info[0]

		switch infoR.Info[0].Status {
		case sAPI.JobStatus_COMPLETED:
			status.ContainerStatuses[0].State.Terminated = &v1.ContainerStateTerminated{
				Reason: "Job finished",
			}
			status.Phase = v1.PodSucceeded
			if pj.jobSpec.Results != nil {
				if err := p.startCollectingResultsPod(pj.pod, pj.jobSpec.Results); err != nil {
					log.Printf("can't collect job results err: %s", err)
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

	var pods []*v1.Pod
	for _, pj := range p.pods {
		pods = append(pods, pj.pod)
	}

	return pods, nil
}

// Capacity returns a resource list containing the capacity limits.
func (p *Provider) Capacity(ctx context.Context) v1.ResourceList {
	return v1.ResourceList{
		"cpu":    resource.MustParse("2"),
		"memory": resource.MustParse("2Gi"),
		"pods":   resource.MustParse("128"),
	}
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), for updates to the node status.
func (p *Provider) NodeConditions(ctx context.Context) []v1.NodeCondition {
	return []v1.NodeCondition{
		{
			Type:               "Ready",
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  metav1.Now(),
			LastTransitionTime: metav1.Now(),
			Reason:             "KubeletReady",
			Message:            "kubelet is ready.",
		},
	}
}

// NodeAddresses returns a list of addresses for the node status
// within Kubernetes.
func (p *Provider) NodeAddresses(ctx context.Context) []v1.NodeAddress {
	return []v1.NodeAddress{
		{
			Type:    "InternalIP",
			Address: p.internalIP,
		},
	}
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *Provider) NodeDaemonEndpoints(ctx context.Context) *v1.NodeDaemonEndpoints {
	return &v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}
}

// OperatingSystem returns the operating system for this provider.
func (p *Provider) OperatingSystem() string {
	return p.operatingSystem
}

// GetStatsSummary returns dummy stats for all pods known by this provider.
func (p *Provider) GetStatsSummary(ctx context.Context) (*stats.Summary, error) {
	// Grab the current timestamp so we can report it as the time the stats were generated.
	time := metav1.NewTime(time.Now())

	// Create the Summary object that will later be populated with node and pod stats.
	res := &stats.Summary{}

	// Populate the Summary object with basic node stats.
	res.Node = stats.NodeStats{
		NodeName:  p.nodeName,
		StartTime: metav1.NewTime(p.startTime),
	}

	// Populate the Summary object with dummy stats for each pod known by this provider.
	for _, pi := range p.pods {
		var (
			// totalUsageNanoCores will be populated with the sum of the values of UsageNanoCores computes across all containers in the pod.
			totalUsageNanoCores uint64
			// totalUsageBytes will be populated with the sum of the values of UsageBytes computed across all containers in the pod.
			totalUsageBytes uint64
		)

		// Create a PodStats object to populate with pod stats.
		pss := stats.PodStats{
			PodRef: stats.PodReference{
				Name:      pi.pod.Name,
				Namespace: pi.pod.Namespace,
				UID:       string(pi.pod.UID),
			},
			StartTime: pi.pod.CreationTimestamp,
		}

		// Iterate over all containers in the current pod to compute dummy stats.
		for _, container := range pi.pod.Spec.Containers {
			// Grab a dummy value to be used as the total CPU usage.
			// The value should fit a uint32 in order to avoid overflows later on when computing pod stats.
			dummyUsageNanoCores := uint64(rand.Uint32())
			totalUsageNanoCores += dummyUsageNanoCores
			// Create a dummy value to be used as the total RAM usage.
			// The value should fit a uint32 in order to avoid overflows later on when computing pod stats.
			dummyUsageBytes := uint64(rand.Uint32())
			totalUsageBytes += dummyUsageBytes
			// Append a ContainerStats object containing the dummy stats to the PodStats object.
			pss.Containers = append(pss.Containers, stats.ContainerStats{
				Name:      container.Name,
				StartTime: pi.pod.CreationTimestamp,
				CPU: &stats.CPUStats{
					Time:           time,
					UsageNanoCores: &dummyUsageNanoCores,
				},
				Memory: &stats.MemoryStats{
					Time:       time,
					UsageBytes: &dummyUsageBytes,
				},
			})
		}

		// Populate the CPU and RAM stats for the pod and append the PodsStats object to the Summary object to be returned.
		pss.CPU = &stats.CPUStats{
			Time:           time,
			UsageNanoCores: &totalUsageNanoCores,
		}
		pss.Memory = &stats.MemoryStats{
			Time:       time,
			UsageBytes: &totalUsageBytes,
		}
		res.Pods = append(res.Pods, pss)
	}

	// Return the dummy stats.
	return res, nil
}

// startCollectingResultsPod creates a new pod which will transfer data from
// slurm cluster to mounted volume.
func (p *Provider) startCollectingResultsPod(pod *v1.Pod, r *v1alpha1.SlurmJobResults) error {
	collectPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pod.Name + "-collect",
			Namespace: pod.Namespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:            "cr1",
					Image:           "cloud.sylabs.io/library/slurm/results:staging",
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
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	collectPod.OwnerReferences = pod.OwnerReferences //allows k8s to delete pod after parent SlurmJob kind be deleted.

	_, err := p.coreC.Pods(pod.Namespace).Create(collectPod)
	return errors.Wrap(err, "can't create collect results pod")
}

func podName(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}
