package dump

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/config"
)

type ConnectionConfigurerBuilder struct{}

func (b *ConnectionConfigurerBuilder) Build(cfg any) (interfaces.ConnectionConfigurer, error) {
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
