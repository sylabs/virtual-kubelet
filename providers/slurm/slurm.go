package slurm

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pkg/errors"
	sAPI "github.com/sylabs/slurm-operator/pkg/workload/api"
	"github.com/sylabs/virtual-kubelet/vkubelet/api"
	"google.golang.org/grpc"
	"io"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	stats "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
	"log"
	"math/rand"
	"net/url"
	"os"
	"time"
)

const (
	SlurmJobKind = "SlurmJob"
)

var (
	vkPodName  = os.Getenv("VK_POD_NAME")
	partition  = os.Getenv("PARTITION")
	redBoxSock = os.Getenv("RED_BOX_SOCK")

	ErrNotSupported = errors.New("not supported")
)

type podInfo struct {
	jobID int64
	pod   *v1.Pod
}

// SlurmProvider implements the virtual-kubelet provider interface by forwarding kubelet calls to a web endpoint.
type SlurmProvider struct {
	startTime time.Time

	nodeName           string
	operatingSystem    string
	endpoint           *url.URL
	daemonEndpointPort int32
	internalIP         string

	slurmAPI sAPI.WorkloadManagerClient

	pods map[string]*podInfo
}

// NewSlurmProvider creates a new SlurmProvider
func NewSLurmProvider(nodeName, operatingSystem, internalIP string, daemonEndpointPort int32) (*SlurmProvider, error) {
	conn, err := grpc.Dial("unix://"+redBoxSock, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("can't connect to %s %s", redBoxSock, err)
	}
	client := sAPI.NewWorkloadManagerClient(conn)

	provider := &SlurmProvider{
		startTime: time.Now(),

		nodeName:           nodeName,
		operatingSystem:    operatingSystem,
		internalIP:         internalIP,
		daemonEndpointPort: daemonEndpointPort,
		slurmAPI:           client,
		pods:               make(map[string]*podInfo),
	}

	return provider, nil
}

// CreatePod accepts a Pod definition and forwards the call to the web endpoint
func (p *SlurmProvider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	log.Println("Create Pod")

	var jobID int64

	if len(pod.OwnerReferences) == 1 && pod.OwnerReferences[0].Kind == SlurmJobKind {
		batchScript := pod.Spec.Containers[0].Args[0]

		resp, err := p.slurmAPI.SubmitJob(ctx, &sAPI.SubmitJobRequest{
			Script: batchScript,
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
		jobID: jobID,
		pod:   pod,
	}

	return nil
}

// UpdatePod accepts a Pod definition and forwards the call to the web endpoint
func (p *SlurmProvider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	pi, ok := p.pods[podName(pod.Namespace, pod.Name)]
	if !ok {
		return errors.New("there is no requested pod")
	}
	pi.pod = pod

	return nil
}

// DeletePod accepts a Pod definition and forwards the call to the web endpoint
func (p *SlurmProvider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	log.Printf("Delete %s", podName(pod.Namespace, pod.Name))
	delete(p.pods, podName(pod.Namespace, pod.Name))
	return nil
}

// GetPod returns a pod by name that is being managed by the web server
func (p *SlurmProvider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	log.Printf("Get Pod %s", podName(namespace, name))
	pj, ok := p.pods[podName(namespace, name)]
	if !ok {
		return nil, errors.New("there is no requested pod")
	}

	return pj.pod, nil
}

// GetContainerLogs returns the logs of a container running in a pod by name.
func (p *SlurmProvider) GetContainerLogs(ctx context.Context, namespace, pName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	log.Printf("GetContainerLogs n:%s pod:%s containerName:%s", namespace, pName, containerName)
	return nil, nil
}

func (p *SlurmProvider) GetPodLogs(ctx context.Context, namespace, pod string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	log.Printf("GetPodLogs n:%s pod:%s", namespace, pod)

	pi, ok := p.pods[podName(namespace, pod)]
	if !ok {
		return nil, errors.New("there is no requested pod")
	}

	if pi.jobID == 0 { //skipping not slurm jobs
		return nil, nil
	}

	infoR, err := p.slurmAPI.JobInfo(ctx, &sAPI.JobInfoRequest{JobId: pi.jobID})
	if err != nil {
		return nil, errors.Wrap(err, "can't get slurm job info")
	}

	openResp, err := p.slurmAPI.OpenFile(ctx, &sAPI.OpenFileRequest{Path: infoR.Info[0].StdOut})
	if err != nil {
		return nil, errors.Wrap(err, "can't open slurm job log file")
	}

	read := func() ([]byte, error) {
		c, err := openResp.Recv()
		if c != nil {
			return c.Content, err
		}

		return nil, err
	}

	close := func() error {
		return openResp.CloseSend()
	}

	return newGRPCReader(read, close)
}

// Get full pod name as defined in the provider context
func (p *SlurmProvider) GetPodFullName(namespace string, pod string) string {
	log.Printf("GetPodFullName n:%s p:%s", namespace, pod)
	return ""
}

// RunInContainer SLURM doesn't support it
func (p *SlurmProvider) RunInContainer(ctx context.Context, namespace, name, container string, cmd []string, attach api.AttachIO) error {
	return ErrNotSupported
}

// GetPodStatus retrieves the status of a given pod by name.
func (p *SlurmProvider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
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
	if ok && pj.jobID != 0 {
		infoR, err := p.slurmAPI.JobInfo(ctx, &sAPI.JobInfoRequest{JobId: pj.jobID})
		if err != nil {
			return nil, errors.Wrapf(err, "can't get status for %s", pj.jobID)
		}

		status.Message = infoR.Info[0].Status.String()
		if status.Message == "COMPLETED" {
			status.Phase = v1.PodSucceeded
		}
	}

	return status, nil
}

// GetPods retrieves a list of all pods scheduled to run.
func (p *SlurmProvider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	log.Println("Get Pods")

	var pods []*v1.Pod
	for _, pj := range p.pods {
		pods = append(pods, pj.pod)
	}

	return pods, nil
}

// Capacity returns a resource list containing the capacity limits
func (p *SlurmProvider) Capacity(ctx context.Context) v1.ResourceList {
	return v1.ResourceList{
		"cpu":    resource.MustParse("2"),
		"memory": resource.MustParse("16Gi"),
		"pods":   resource.MustParse("128"),
	}
}

// NodeConditions returns a list of conditions (Ready, OutOfDisk, etc), for updates to the node status
func (p *SlurmProvider) NodeConditions(ctx context.Context) []v1.NodeCondition {
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
func (p *SlurmProvider) NodeAddresses(ctx context.Context) []v1.NodeAddress {
	return []v1.NodeAddress{
		{
			Type:    "InternalIP",
			Address: p.internalIP,
		},
	}
}

// NodeDaemonEndpoints returns NodeDaemonEndpoints for the node status
// within Kubernetes.
func (p *SlurmProvider) NodeDaemonEndpoints(ctx context.Context) *v1.NodeDaemonEndpoints {
	return &v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{
			Port: p.daemonEndpointPort,
		},
	}
}

// OperatingSystem returns the operating system for this provider.
func (p *SlurmProvider) OperatingSystem() string {
	return p.operatingSystem
}

// GetStatsSummary returns dummy stats for all pods known by this provider.
func (p *SlurmProvider) GetStatsSummary(ctx context.Context) (*stats.Summary, error) {
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

func podName(namespace, name string) string {
	return fmt.Sprintf("%s-%s", namespace, name)
}

type grpcReader struct {
	*bytes.Buffer

	read  func() ([]byte, error)
	close func() error
}

func newGRPCReader(read func() ([]byte, error), close func() error) (*grpcReader, error) {
	reader := &grpcReader{
		Buffer: &bytes.Buffer{},
		read:   read,
		close:  close,
	}

	go reader.startRead()

	return reader, nil
}

func (r *grpcReader) Close() error {
	if r.close != nil {
		if err := r.close(); err != nil {
			return errors.Wrap(err, "inside close callback")
		}
	}

	return nil
}

func (r *grpcReader) startRead() {
	for {
		b, err := r.read()
		if b != nil && len(b) != 0 {
			r.Buffer.Write(b)
		}

		if err != nil {
			log.Printf("grpc reader finished from err: %s", err)
			return
		}
	}
}
