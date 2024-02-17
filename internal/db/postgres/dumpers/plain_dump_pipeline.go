// Copyright 2023 Greenmask
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

package dumpers

import (
	"context"
	"fmt"
	"io"

	dump "github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
)

type PlainDumpPipeline struct {
	w     io.Writer
	line  uint64
	table *dump.Table
}

func NewPlainDumpPipeline(table *dump.Table, w io.Writer) *PlainDumpPipeline {
	return &PlainDumpPipeline{
		table: table,
		w:     w,
	}
}

func (pdp *PlainDumpPipeline) Init(ctx context.Context) error {
	return nil
}

func (pdp *PlainDumpPipeline) Dump(ctx context.Context, data []byte) (err error) {
	pdp.line++
	if _, err := pdp.w.Write(data); err != nil {
		return NewDumpError(pdp.table.Schema, pdp.table.Name, pdp.line, err)
	}
	return nil
}

func (pdp *PlainDumpPipeline) Done(ctx context.Context) error {
	return nil
}

func (pdp *PlainDumpPipeline) CompleteDump() (err error) {
	res := make([]byte, 0, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = pdp.w.Write(res)
	if err != nil {
		return NewDumpError(pdp.table.Schema, pdp.table.Name, pdp.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}
