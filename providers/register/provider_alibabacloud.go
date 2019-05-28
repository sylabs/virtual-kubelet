// +build alibabacloud_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/alibabacloud"
)

func init() {
	register("alibabacloud", aliCloudInit)
}

func aliCloudInit(cfg InitConfig) (providers.Provider, error) {
	return alibabacloud.NewECIProvider(
		cfg.ConfigPath,
		cfg.ResourceManager,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
