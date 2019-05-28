// +build huawei_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/huawei"
)

func init() {
	register("huawei", initHuawei)
}

func initHuawei(cfg InitConfig) (providers.Provider, error) {
	return huawei.NewCCIProvider(
		cfg.ConfigPath,
		cfg.ResourceManager,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
