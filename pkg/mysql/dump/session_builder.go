package dump

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
)

type DumpSessionBuilder struct{}

func (b *DumpSessionBuilder) Open(ctx context.Context, cfg core.ConnectionConfigurer) (core.DatabaseSession, error) {
	c, ok := cfg.ConnectionConfig().(*DumpConnectionConfig)
	if !ok {
		return nil, fmt.Errorf("unexpected connection config type %T, want *DumpConnectionConfig", cfg.ConnectionConfig())
	}

	connCfg, err := c.MySQL.ConnectionConfig(c.Common.SSL)
	if err != nil {
		return nil, fmt.Errorf("build mysql connection config: %w", err)
	}

	var poolOpts []pool.Option
	if c.MySQL.PoolHeartbeatInterval > 0 {
		poolOpts = append(poolOpts, pool.WithHeartbeat(c.MySQL.PoolHeartbeatInterval))
	}
	if c.MySQL.PoolHeartbeatTimeout > 0 {
		poolOpts = append(poolOpts, pool.WithHeartbeatTimeout(c.MySQL.PoolHeartbeatTimeout))
	}

	// The pool is itself the DatabaseSession implementation. Init establishes the
	// consistent snapshot across all worker connections and the shared meta
	// transaction; on failure it cleans up the connections it managed to open, so
	// the caller does not need to Close a failed pool.
	p := pool.NewConsistentTxPool(connCfg, c.ConnectionPoolSize, poolOpts...)
	if err := p.Init(ctx); err != nil {
		return nil, fmt.Errorf("init mysql dump session pool: %w", err)
	}
	return p, nil
}
