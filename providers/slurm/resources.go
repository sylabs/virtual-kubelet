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
	opAdd    = "add"
	opRemove = "remove"
)

type operation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value"`
}

// nodePatcher provides convenient API for patching k8s node labels and resources
type nodePatcher struct {
	coreClient *corev1.CoreV1Client
}

// newNodePatcher creates new nodePatcher
func newNodePatcher(c *corev1.CoreV1Client) (*nodePatcher, error) {
	return &nodePatcher{coreClient: c}, nil
}

// AddNodeResources adds passed resources to node capacity.
func (c *nodePatcher) AddNodeResources(nodeName string, resources map[string]int) error {
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
func (c *nodePatcher) AddNodeLabels(nodeName string, labels map[string]string) error {
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

// RemoveNodeLabels removes nodes labels from node
func (c *nodePatcher) RemoveNodeLabels(nodeName string, labels map[string]string) error {
	const k8sLabelT = "/metadata/labels/slurm.sylabs.io~1%s"

	ops := make([]operation, 0, len(labels))
	for k := range labels {
		op := operation{
			Op:   opRemove,
			Path: fmt.Sprintf(k8sLabelT, k),
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
	k8s       *nodePatcher
	slurmC    api.WorkloadManagerClient
	partition string

	prevFeatures []*api.Feature
}

func newWatchDog(k8s *nodePatcher, c api.WorkloadManagerClient, partition string) *watchDog {
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

		// clean up old labels
		labelsToRemove := make(map[string]string)
		for _, f := range wd.prevFeatures {
			labelsToRemove[getFeatureKey(f)] = strconv.FormatInt(f.Quantity, 10)
		}
		if len(labelsToRemove) != 0 {
			if err := wd.k8s.RemoveNodeLabels(vkPodName, labelsToRemove); err != nil {
				log.Printf("Can't remove old feature labels")
			}
		}

		labels := map[string]string{
			"nodes":        strconv.FormatInt(resResp.Nodes, 10),
			"wall-time":    strconv.FormatInt(resResp.WallTime, 10),
			"cpu-per-node": strconv.FormatInt(resResp.CpuPerNode, 10),
			"mem-per-node": strconv.FormatInt(resResp.MemPerNode, 10),
		}

		for _, f := range resResp.Features {
			labels[getFeatureKey(f)] = strconv.FormatInt(f.Quantity, 10)
		}

		if err := wd.k8s.AddNodeLabels(vkPodName, labels); err != nil {
			log.Printf("can't add node labes err: %s", err)
		}

		wd.prevFeatures = resResp.Features
	}
}

func getFeatureKey(f *api.Feature) string {
	if f.Version == "" {
		return f.Name
	}

	return fmt.Sprintf("%s_%s", f.Name, f.Version)
}
