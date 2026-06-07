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
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// ConsistentTxPool is itself the MySQL implementation of core.DumpSession
// consumed by the common dump pipeline — there is no separate adapter type.
//
// The pipeline opens one session at the start of a dump and shares it across all
// planning and execution stages so they observe a single consistent database
// state. For MySQL that consistent state is the snapshot the pool establishes
// during Init:
//
//   - RunWithOperationalDB scopes the shared snapshot meta-transaction to a
//     callback, used by the generic stages (introspection, metadata queries,
//     validation).
//   - RunWithEngineResource borrows a per-worker connection bound to the same
//     snapshot for the duration of a callback, so MySQL-specific dumpers can read
//     table data without ever leaking a pooled connection.
//
// Close (defined in pool.go) completes the contract: it rolls back the snapshot
// transactions and releases all connections.
var _ core.DumpSession = (*ConsistentTxPool)(nil)

// RunWithOperationalDB invokes fn with the shared snapshot meta-transaction as a
// generic DB.
//
// All generic planning stages read through this single transaction so they see
// exactly the snapshot captured at Init. The meta-transaction lives for the whole
// pool lifetime (it is not per-call acquired/released), so this is a thin scoping
// wrapper; routing through it keeps every operational read under the session's
// control. It is read-oriented; engine-specific dumping uses per-worker
// connections via RunWithEngineResource instead.
func (p *ConsistentTxPool) RunWithOperationalDB(ctx context.Context, fn func(ctx context.Context, db core.DB) error) error {
	if p.metaTx == nil {
		return fmt.Errorf("dump session: pool is not initialised")
	}
	return fn(ctx, p.GetMetaTx())
}

// RunWithEngineResource borrows a pooled connection bound to the dump snapshot
// for the duration of fn and returns it to the pool afterwards (even on error or
// panic), via the leak-safe RunWithConn. The resource passed to fn is a
// WorkerConn; consumers type-assert it.
func (p *ConsistentTxPool) RunWithEngineResource(ctx context.Context, fn func(ctx context.Context, res any) error) error {
	if p.queue == nil {
		return fmt.Errorf("dump session: pool is not initialised")
	}
	return p.RunWithConn(ctx, func(ctx context.Context, conn WorkerConn) error {
		return fn(ctx, conn)
	})
}
