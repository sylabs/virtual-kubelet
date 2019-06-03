// +build slurm_provider

package register

import (
	"github.com/virtual-kubelet/virtual-kubelet/providers"
	"github.com/virtual-kubelet/virtual-kubelet/providers/slurm"
)

func init() {
	register("slurm", initSlurm)
}

func initSlurm(cfg InitConfig) (providers.Provider, error) {
	return slurm.NewProvider(
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
