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

package pool

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

var _ core.RestoreSession = (*RestoreSessionSingleTx)(nil)

// RestoreSessionSingleTx is the single-transaction restore session: Init begins
// one transaction per connection and every RunWithEngineResource call runs inside
// the connection's shared transaction. The whole restore is finalized atomically
// by DoneWithError — commit when the run succeeded, rollback when it failed.
type RestoreSessionSingleTx struct {
	restoreConnPool
}

func NewRestoreSessionSingleTx(cfg *mysqlmodels.ConnConfig, poolSize int) *RestoreSessionSingleTx {
	return &RestoreSessionSingleTx{restoreConnPool: newRestoreConnPool(cfg, poolSize)}
}

// Init opens the database and connections and begins one transaction per connection.
func (s *RestoreSessionSingleTx) Init(ctx context.Context) error {
	return s.initConns(ctx, true)
}

// RunWithEngineResource borrows a connection and runs fn against its shared
// transaction. No per-call commit/rollback — finalization happens in DoneWithError.
func (s *RestoreSessionSingleTx) RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error {
	return s.withConn(ctx, func(ctx context.Context, rc *restoreWorkerConn) error {
		return fn(ctx, rc)
	})
}

// DoneWithError finalizes every connection's transaction: commit when cause is
// nil, rollback otherwise.
func (s *RestoreSessionSingleTx) DoneWithError(_ context.Context, cause error) error {
	return s.eachConn(func(rc *restoreWorkerConn) error {
		if cause != nil {
			return rc.rollback()
		}
		return rc.commit()
	})
}
