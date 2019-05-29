package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/slurm"
)

func init() {
	register("slurm", initSlurm)
}

func initSlurm(cfg InitConfig) (providers.Provider, error) {
	return slurm.NewSLURMProvider(
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
