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
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

var _ core.RestoreSession = (*RestoreSessionDefault)(nil)

// RestoreSessionDefault is the per-call restore session: each RunWithEngineResource
// call runs inside its own transaction, committed when the wrapped function
// succeeds and rolled back when it fails. There is no global transaction, so
// DoneWithError is a no-op — every unit of work has already been finalized.
type RestoreSessionDefault struct {
	restoreConnPool
}

func NewRestoreSessionDefault(cfg *mysqlmodels.ConnConfig, poolSize int) *RestoreSessionDefault {
	return &RestoreSessionDefault{restoreConnPool: newRestoreConnPool(cfg, poolSize)}
}

// Init opens the database and connections without beginning transactions.
func (s *RestoreSessionDefault) Init(ctx context.Context) error {
	return s.initConns(ctx, false)
}

// RunWithEngineResource borrows a connection, begins a transaction, runs fn, and
// commits on success or rolls back on failure.
func (s *RestoreSessionDefault) RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error {
	return s.withConn(ctx, func(ctx context.Context, rc *restoreWorkerConn) error {
		if err := rc.begin(ctx); err != nil {
			return err
		}
		if err := fn(ctx, rc); err != nil {
			if rbErr := rc.rollback(); rbErr != nil {
				return errors.Join(err, rbErr)
			}
			return err
		}
		if err := rc.commit(); err != nil {
			return fmt.Errorf("commit per-call tx: %w", err)
		}
		return nil
	})
}

// DoneWithError is a no-op: per-call transactions are finalized inside each
// RunWithEngineResource call, so there is no global transaction to commit or
// roll back.
func (s *RestoreSessionDefault) DoneWithError(_ context.Context, _ error) error {
	return nil
}
