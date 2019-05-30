package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/sylabs/slurm-operator/pkg/workload/api"

	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/types"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	opAdd = "add"
)

type operation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// k8sClient provides convenient API for interacting with k8s core API.
type k8sClient struct {
	coreClient *corev1.CoreV1Client
}

// newK8SClient fetches k8s config and initializes core client based on it.
func newK8SClient(c *corev1.CoreV1Client) (*k8sClient, error) {
	return &k8sClient{coreClient: c}, nil
}

// AddNodeResources adds passed resources to node capacity.
func (c *k8sClient) AddNodeResources(nodeName string, resources map[string]int) error {
	// https://kubernetes.io/docs/tasks/administer-cluster/extended-resource-node/
	const k8sResourceT = "/status/capacity/slurm.sylabs.io~1%s"

	ops := make([]operation, 0, len(resources))
	for k, v := range resources {
		op := operation{
			Op:    opAdd,
			Path:  fmt.Sprintf(k8sResourceT, k),
			Value: strconv.Itoa(v),
		}
		ops = append(ops, op)
	}
	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(ops); err != nil {
		return errors.Wrap(err, "could not encode resources patch")
	}

	_, err := c.coreClient.Nodes().Patch(nodeName, types.JSONPatchType, buff.Bytes(), "status")
	if err != nil {
		return errors.Wrap(err, "could not patch node resources")
	}
	return nil
}

// AddNodeLabels adds passed labels to node labels.
func (c *k8sClient) AddNodeLabels(nodeName string, labels map[string]string) error {
	const k8sLabelT = "/metadata/labels/slurm.sylabs.io~1%s"

	ops := make([]operation, 0, len(labels))
	for k, v := range labels {
		op := operation{
			Op:    opAdd,
			Path:  fmt.Sprintf(k8sLabelT, k),
			Value: v,
		}
		ops = append(ops, op)
	}

	var buff bytes.Buffer
	if err := json.NewEncoder(&buff).Encode(ops); err != nil {
		return errors.Wrap(err, "could not encode labels patch")
	}

	_, err := c.coreClient.Nodes().Patch(nodeName, types.JSONPatchType, buff.Bytes())
	if err != nil {
		return errors.Wrap(err, "could not patch node labels")
	}
	return nil
}

type watchDog struct {
	k8s       *k8sClient
	slurmC    api.WorkloadManagerClient
	partition string
}

func newWatchDog(k8s *k8sClient, c api.WorkloadManagerClient, partition string) *watchDog {
	return &watchDog{
		k8s:       k8s,
		slurmC:    c,
		partition: partition,
	}
}

func (wd *watchDog) watch() {
	log.Println("Watch dog started")
	for {
		time.Sleep(10 * time.Second)
		resResp, err := wd.slurmC.Resources(context.Background(), &api.ResourcesRequest{Partition: partition})
		if err != nil {
			log.Printf("can't get resources err: %s", err)
			continue
		}

		if err := wd.k8s.AddNodeLabels(vkPodName, map[string]string{
			"nodes":        strconv.FormatInt(resResp.Nodes, 10),
			"wall-time":    strconv.FormatInt(resResp.WallTime, 10),
			"cpu-per-node": strconv.FormatInt(resResp.CpuPerNode, 10),
			"mem-per-node": strconv.FormatInt(resResp.MemPerNode, 10),
		}); err != nil {
			log.Printf("can't add node labes err: %s", err)
		}
	}
}
