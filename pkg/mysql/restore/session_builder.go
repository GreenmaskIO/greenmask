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

package restore

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/pool"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/connconfig"
)

var _ core.DatabaseSessionBuilder = (*RestoreSessionBuilder)(nil)

// RestoreSessionBuilder opens a RestorePool for a MySQL restore session.
// It type-asserts cfg.ConnectionConfig() to *connconfig.RestoreConnectionConfig directly —
// no intermediate interface — so the pool package stays DBMS-agnostic.
type RestoreSessionBuilder struct{}

func (b *RestoreSessionBuilder) Open(ctx context.Context, cfg core.ConnectionConfigurer) (core.DatabaseSession, error) {
	cc, ok := cfg.ConnectionConfig().(*connconfig.RestoreConnectionConfig)
	if !ok {
		return nil, fmt.Errorf("restore session builder: expected *connconfig.RestoreConnectionConfig, got %T", cfg.ConnectionConfig())
	}
	connCfg, err := cc.MySQL.ConnectionConfig(cc.Common.SSL)
	if err != nil {
		return nil, fmt.Errorf("build mysql restore connection config: %w", err)
	}
	// Construct only — the restore processor owns the session lifecycle and calls
	// Init / DoneWithError around the run.
	if cc.Common.SingleTransaction {
		return pool.NewRestoreSessionSingleTx(connCfg, cc.ConnectionPoolSize), nil
	}
	return pool.NewRestoreSessionDefault(connCfg, cc.ConnectionPoolSize), nil
}
