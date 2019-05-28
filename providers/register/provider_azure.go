// +build azure_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/azure"
)

func init() {
	register("azure", initAzure)
}

func initAzure(cfg InitConfig) (providers.Provider, error) {
	return azure.NewACIProvider(
		cfg.ConfigPath,
		cfg.ResourceManager,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
