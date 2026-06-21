// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dump

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
)

var _ core.DatabaseSessionBuilder = (*DumpSessionBuilder)(nil)

// DumpSessionBuilder opens a ConsistentTxPool for a MySQL dump session.
// It type-asserts cfg.ConnectionConfig() to *DumpConnectionConfig directly —
// no intermediate interface — so the pool package stays DBMS-agnostic.
type DumpSessionBuilder struct{}

func (b *DumpSessionBuilder) Open(ctx context.Context, cfg core.ConnectionConfigurer) (core.DatabaseSession, error) {
	cc, ok := cfg.ConnectionConfig().(*DumpConnectionConfig)
	if !ok {
		return nil, fmt.Errorf("dump session builder: expected *DumpConnectionConfig, got %T", cfg.ConnectionConfig())
	}
	connCfg, err := cc.MySQL.ConnectionConfig(cc.Common.SSL)
	if err != nil {
		return nil, fmt.Errorf("build mysql dump connection config: %w", err)
	}
	var opts []pool.Option
	if cc.MySQL.PoolHeartbeatInterval > 0 {
		opts = append(opts, pool.WithHeartbeat(cc.MySQL.PoolHeartbeatInterval))
	}
	if cc.MySQL.PoolHeartbeatTimeout > 0 {
		opts = append(opts, pool.WithHeartbeatTimeout(cc.MySQL.PoolHeartbeatTimeout))
	}
	p := pool.NewConsistentTxPool(connCfg, cc.ConnectionPoolSize, opts...)
	if err := p.Init(ctx); err != nil {
		return nil, fmt.Errorf("init mysql dump session pool: %w", err)
	}
	return p, nil
}
