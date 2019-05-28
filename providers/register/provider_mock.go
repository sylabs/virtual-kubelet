// +build mock_provider

package register

import (
	"github.com/sylabs/virtual-kubelet/providers"
	"github.com/sylabs/virtual-kubelet/providers/mock"
)

func init() {
	register("mock", initMock)
}

func initMock(cfg InitConfig) (providers.Provider, error) {
	return mock.NewMockProvider(
		cfg.ConfigPath,
		cfg.NodeName,
		cfg.OperatingSystem,
		cfg.InternalIP,
		cfg.DaemonPort,
	)
}
