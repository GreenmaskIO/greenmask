package dump

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

type ConnectionConfigurerBuilder struct{}

func (b *ConnectionConfigurerBuilder) Build(cfg any) (core.ConnectionConfigurer, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	poolSize := c.Dump.Options.Jobs
	if poolSize <= 0 {
		poolSize = 1
	}
	return &DumpConnectionConfig{
		Common:             c.Dump.Options,
		MySQL:              c.Dump.MysqlConfig,
		ConnectionPoolSize: poolSize,
	}, nil
}
