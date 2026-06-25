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

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/dumpers"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/common/record"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

var _ core.ObjectDumpFactory = (*Factory)(nil)

// Factory builds MySQL table data dumpers. It is constructed once per run and
// carries only static dump-format options; the runtime resources (dump session
// and storage) are injected into the produced dumper at execution time via
// ObjectDumper.Dump.
type Factory struct {
	opts []Option
}

// NewFactory creates a table dump factory. opts configure the per-table writer
// (compression, pgzip, hex-encoding of binary columns).
func NewFactory(opts ...Option) *Factory {
	return &Factory{opts: opts}
}

func (f *Factory) Kind() core.ObjectKind {
	return kinds.ObjectKindTable
}

// New builds a table data dumper from the spec. When the table has transformers
// configured it returns a pipeline-backed TableDumper that decodes each row,
// applies the transformation pipeline and re-encodes it; otherwise it returns a
// TableRawDumper that streams rows as-is into INSERT value tuples.
func (f *Factory) New(spec core.ObjectDumpSpec) (core.ObjectDumper, error) {
	if spec.Kind != kinds.ObjectKindTable {
		return nil, fmt.Errorf("expected kind %q, got %q", kinds.ObjectKindTable, spec.Kind)
	}
	payload, ok := spec.Payload.(transformercontext.TableDumpContext)
	if !ok {
		return nil, fmt.Errorf("expected transformercontext.TableDumpContext payload, got %T", spec.Payload)
	}
	if payload.Table == nil {
		return nil, fmt.Errorf("table is not set in dump spec payload")
	}

	tr := NewTableDataReader(payload.Table, payload.Query)
	// Per-table writer options carried on the spec payload (e.g. compression)
	// take precedence over the factory defaults, mirroring the schema dump path.
	writerOpts := append(append([]Option{}, f.opts...), WithCompression(payload.Compression))
	tw := NewTableDataWriter(*payload.Table, writerOpts...)

	if !payload.HasTransformer() {
		return dumpers.NewTableRawDumper(spec.TaskID, tr, tw, payload.Table), nil
	}

	// Per-record decoder bound to the table driver, plus the table context which
	// is itself the transformation pipeline (core.Pipeliner).
	rawRecord := rawrecord.NewRawRecord(len(payload.Table.Columns), core.NullValueSeq)
	r := record.NewRecord(rawRecord, payload.TableDriver)
	dumper, err := dumpers.NewTableDumper(spec.TaskID, tr, tw, r, &payload, payload.Table)
	if err != nil {
		return nil, fmt.Errorf("create table dumper: %w", err)
	}
	return dumper, nil
}
