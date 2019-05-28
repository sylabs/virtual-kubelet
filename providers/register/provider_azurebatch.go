// +build azurebatch_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/azurebatch"
)

func init() {
	register("azurebatch", initAzureBatch)
}

func initAzureBatch(cfg InitConfig) (providers.Provider, error) {
	return azurebatch.NewBatchProvider(
		cfg.ConfigPath,
		cfg.ResourceManager,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
