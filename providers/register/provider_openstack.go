// +build openstack_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/openstack"
)

func init() {
	register("openstack", initOpenStack)
}

func initOpenStack(cfg InitConfig) (providers.Provider, error) {
	return openstack.NewZunProvider(
		cfg.ConfigPath,
		cfg.ResourceManager,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.DaemonPort)
}
