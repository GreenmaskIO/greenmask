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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

// The mysqldump/mysql CLI mocks are shared across feature tests; see
// pkg/testutils/cmd.go. These aliases keep existing &CmdRunnerMock{} usages.
type (
	CmdRunnerMock   = testutils.CmdRunnerMock
	CmdProducerMock = testutils.CmdProducerMock
)

// sharedStorageProvisioner is a RestoreStorageProvisioner that hands the restore
// pipeline the same in-memory storage the dump wrote to. The validate.Storage is
// flat (it keys objects by filename regardless of dumpID sub-scoping), so the
// dumpID is irrelevant and the storage is returned as-is.
type sharedStorageProvisioner struct {
	st *validate.Storage
}

var _ core.RestoreStorageProvisioner = (*sharedStorageProvisioner)(nil)

func (p *sharedStorageProvisioner) Provision(
	_ context.Context,
	_ any,
	_ core.DumpID,
) (core.Storager, error) {
	return p.st, nil
}
