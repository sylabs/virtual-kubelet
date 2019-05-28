package vkubelet

import (
	"context"
	"path"
	"testing"

	"github.com/cpuguy83/strongerrors"
	pkgerrors "github.com/pkg/errors"
	testutil "github.com/sylabs/virtual-kubelet/test/util"
	"gotest.tools/assert"
	is "gotest.tools/assert/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type mockProvider struct {
	pods map[string]*corev1.Pod

	creates int
	updates int
	deletes int
}

func (m *mockProvider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	m.pods[path.Join(pod.GetNamespace(), pod.GetName())] = pod
	m.creates++
	return nil
}

func (m *mockProvider) UpdatePod(ctx context.Context, pod *corev1.Pod) error {
	m.pods[path.Join(pod.GetNamespace(), pod.GetName())] = pod
	m.updates++
	return nil
}

func (m *mockProvider) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	p := m.pods[path.Join(namespace, name)]
	if p == nil {
		return nil, strongerrors.NotFound(pkgerrors.New("not found"))
	}
	return p, nil
}

func (m *mockProvider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	p := m.pods[path.Join(namespace, name)]
	if p == nil {
		return nil, strongerrors.NotFound(pkgerrors.New("not found"))
	}
	return &p.Status, nil
}

func (m *mockProvider) DeletePod(ctx context.Context, p *corev1.Pod) error {
	delete(m.pods, path.Join(p.GetNamespace(), p.GetName()))
	m.deletes++
	return nil
}

func (m *mockProvider) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	ls := make([]*corev1.Pod, 0, len(m.pods))
	for _, p := range ls {
		ls = append(ls, p)
	}
	return ls, nil
}

type TestServer struct {
	*Server
	mock   *mockProvider
	client *fake.Clientset
}

func newMockProvider() *mockProvider {
	return &mockProvider{pods: make(map[string]*corev1.Pod)}
}

func newTestServer() *TestServer {
	fk8s := fake.NewSimpleClientset()

	rm := testutil.FakeResourceManager()
	p := newMockProvider()

	tsvr := &TestServer{
		Server: &Server{
			namespace:       "default",
			nodeName:        "vk123",
			provider:        p,
			resourceManager: rm,
			k8sClient:       fk8s,
		},
		mock:   p,
		client: fk8s,
	}
	return tsvr
}

func TestPodHashingEqual(t *testing.T) {
	p1 := corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12-perl",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	h1 := hashPodSpec(p1)

	p2 := corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12-perl",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	h2 := hashPodSpec(p2)
	assert.Check(t, is.Equal(h1, h2))
}

func TestPodHashingDifferent(t *testing.T) {
	p1 := corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	h1 := hashPodSpec(p1)

	p2 := corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12-perl",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	h2 := hashPodSpec(p2)
	assert.Check(t, h1 != h2)
}

func TestPodCreateNewPod(t *testing.T) {
	svr := newTestServer()

	pod := &corev1.Pod{}
	pod.ObjectMeta.Namespace = "default"
	pod.ObjectMeta.Name = "nginx"
	pod.Spec = corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	er := testutil.FakeEventRecorder(5)
	err := svr.createOrUpdatePod(context.Background(), pod, er)
	assert.Check(t, is.Nil(err))
	// createOrUpdate called CreatePod but did not call UpdatePod because the pod did not exist
	assert.Check(t, is.Equal(svr.mock.creates, 1))
	assert.Check(t, is.Equal(svr.mock.updates, 0))
}

func TestPodUpdateExisting(t *testing.T) {
	svr := newTestServer()

	pod := &corev1.Pod{}
	pod.ObjectMeta.Namespace = "default"
	pod.ObjectMeta.Name = "nginx"
	pod.Spec = corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	err := svr.provider.CreatePod(context.Background(), pod)
	assert.Check(t, is.Nil(err))
	assert.Check(t, is.Equal(svr.mock.creates, 1))
	assert.Check(t, is.Equal(svr.mock.updates, 0))

	pod2 := &corev1.Pod{}
	pod2.ObjectMeta.Namespace = "default"
	pod2.ObjectMeta.Name = "nginx"
	pod2.Spec = corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12-perl",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	er := testutil.FakeEventRecorder(5)
	err = svr.createOrUpdatePod(context.Background(), pod2, er)
	assert.Check(t, is.Nil(err))

	// createOrUpdate didn't call CreatePod but did call UpdatePod because the spec changed
	assert.Check(t, is.Equal(svr.mock.creates, 1))
	assert.Check(t, is.Equal(svr.mock.updates, 1))
}

func TestPodNoSpecChange(t *testing.T) {
	svr := newTestServer()

	pod := &corev1.Pod{}
	pod.ObjectMeta.Namespace = "default"
	pod.ObjectMeta.Name = "nginx"
	pod.Spec = corev1.PodSpec{
		Containers: []corev1.Container{
			corev1.Container{
				Name:  "nginx",
				Image: "nginx:1.15.12",
				Ports: []corev1.ContainerPort{
					corev1.ContainerPort{
						ContainerPort: 443,
						Protocol:      "tcp",
					},
				},
			},
		},
	}

	err := svr.mock.CreatePod(context.Background(), pod)
	assert.Check(t, is.Nil(err))
	assert.Check(t, is.Equal(svr.mock.creates, 1))
	assert.Check(t, is.Equal(svr.mock.updates, 0))

	er := testutil.FakeEventRecorder(5)
	err = svr.createOrUpdatePod(context.Background(), pod, er)
	assert.Check(t, is.Nil(err))

	// createOrUpdate didn't call CreatePod or UpdatePod, spec didn't change
	assert.Check(t, is.Equal(svr.mock.creates, 1))
	assert.Check(t, is.Equal(svr.mock.updates, 0))
}
