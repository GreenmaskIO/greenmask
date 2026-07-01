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

package table

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

var _ core.ObjectRestoreFactory = (*Factory)(nil)

// Factory builds TableRestorer instances from ObjectRestoreSpecs.
// It is the symmetric counterpart of the dump-side table Factory.
type Factory struct{}

func NewFactory() *Factory { return &Factory{} }

func (f *Factory) Kind() core.ObjectKind { return kinds.ObjectKindTable }

// New assembles a TableRestorer from the spec by pairing a TableRestoreReader
// (reads the dump file from storage) with the appropriate RestoreRowWriter
// (writes to the target DB). Currently only INSERT format is enabled.
func (f *Factory) New(spec core.ObjectRestoreSpec) (core.ObjectRestorer, error) {
	table, ok := spec.Payload.(*core.Table)
	if !ok {
		return nil, fmt.Errorf("expected *core.Table payload, got %T", spec.Payload)
	}

	reader := NewTableRestoreReader(spec.Filename, spec.Compression)

	switch spec.Format {
	case core.DumpFormatInsert:
		writer := NewInsertRestoreWriter(table)
		return NewTableRestorer(spec, table, reader, writer), nil
	default:
		return nil, fmt.Errorf(
			"unsupported dump format %q for table %s.%s",
			spec.Format, table.Schema, table.Name,
		)
	}
}
