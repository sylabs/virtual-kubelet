package wlm

import (
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sylabs/wlm-operator/pkg/operator/client/clientset/versioned"
	sAPI "github.com/sylabs/wlm-operator/pkg/workload/api"
	"google.golang.org/grpc"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
)

var (
	vkHostNode   = os.Getenv("VK_HOST_NAME")
	vkPodName    = os.Getenv("VK_POD_NAME")
	partition    = os.Getenv("PARTITION")
	redBoxSock   = os.Getenv("RED_BOX_SOCK")
	resultsImage = os.Getenv("RESULTS_IMAGE")

	ErrNotSupported = errors.New("not supported")
	ErrPodNotFound  = errors.New("there is no requested pod")
)

// Provider implements the virtual-kubelet provider interface by forwarding kubelet calls to a slurm cluster.
type Provider struct {
	startTime time.Time
	uid       int64
	gid       int64

	nodeName           string
	operatingSystem    string
	daemonEndpointPort int32
	internalIP         string

	wlmAPI     sAPI.WorkloadManagerClient
	coreClient *corev1.CoreV1Client
	wlmClient  *versioned.Clientset

	pods map[string]*podInfo
}

// NewProvider creates a new SlurmProvider.
// Starts watch dog for updating nodes resources.
func NewProvider(nodeName, operatingSystem, internalIP string, daemonEndpointPort int32) (*Provider, error) {
	conn, err := grpc.Dial("unix://"+redBoxSock, grpc.WithInsecure())
	if err != nil {
		return nil, errors.Wrapf(err, "could not connect to %s", redBoxSock)
	}
	redBoxClient := sAPI.NewWorkloadManagerClient(conn)

	// getting k8s config.
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, errors.Wrap(err, "could not fetch cluster config")
	}

	// corev1 client set is required to create collecting results pod.
	coreClient, err := corev1.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "could not create core client")
	}

	nodePatcher, err := newNodePatcher(coreClient)
	if err != nil {
		return nil, errors.Wrap(err, "could not create nodePatcher")
	}

	// start updating nodes labels (cpu per node, mem per node, nodes, features).
	go newWatchDog(nodePatcher, redBoxClient, partition).watch()

	wlmClient, err := versioned.NewForConfig(config)
	if err != nil {
		return nil, errors.Wrap(err, "could not create wlm client set")
	}

	provider := &Provider{
		startTime: time.Now(),
		uid:       int64(os.Getuid()),
		gid:       int64(os.Getgid()),

		nodeName:           nodeName,
		operatingSystem:    operatingSystem,
		daemonEndpointPort: daemonEndpointPort,
		internalIP:         internalIP,

		wlmAPI:     redBoxClient,
		coreClient: coreClient,
		wlmClient:  wlmClient,

		pods: make(map[string]*podInfo),
	}

	return provider, nil
}
