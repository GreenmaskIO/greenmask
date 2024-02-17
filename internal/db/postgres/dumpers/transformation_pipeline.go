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
	"slices"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	dump "github.com/greenmaskio/greenmask/internal/db/postgres/dump_objects"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var endOfLineSeq = []byte("\n")

type TransformationFunc func(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)

type TransformationPipeline struct {
	table *dump.Table
	//buf                   *bytes.Buffer
	w                     io.Writer
	line                  uint64
	row                   *pgcopy.Row
	transformationWindows []*TransformationWindow
	Transform             TransformationFunc
	isAsync               bool
	record                *toolkit.Record
}

func NewTransformationPipeline(ctx context.Context, eg *errgroup.Group, table *dump.Table, w io.Writer) (*TransformationPipeline, error) {

	var tws []*TransformationWindow
	var isAsync bool

	// TODO: Fix this hint. Async execution cannot be performed with template record because it is unsafe.
	//       For overcoming it - implement sequence transformer wrapper - that wraps internal (non CMD) transformers
	hasTemplateRecordTransformer := slices.ContainsFunc(table.Transformers, func(transformer utils.Transformer) bool {
		_, ok := transformer.(*transformers.TemplateRecordTransformer)
		return ok
	})

	if !hasTemplateRecordTransformer && table.HasCustomTransformer() && len(table.Transformers) > 1 {
		isAsync = true
		tw := NewTransformationWindow(ctx, eg)
		tws = append(tws, tw)
		for _, t := range table.Transformers {
			if !tw.TryAdd(table, t) {
				tw = NewTransformationWindow(ctx, eg)
				tws = append(tws, tw)
				tw.TryAdd(table, t)
			}
		}
	}

	tp := &TransformationPipeline{
		table: table,
		//buf:                   bytes.NewBuffer(nil),
		w:                     w,
		row:                   pgcopy.NewRow(len(table.Columns)),
		transformationWindows: tws,
		isAsync:               true,
		record:                toolkit.NewRecord(table.Driver),
	}

	var tf TransformationFunc = tp.TransformSync
	if isAsync {
		tf = tp.TransformAsync
	}
	tp.Transform = tf

	return tp, nil
}

func (tp *TransformationPipeline) Init(ctx context.Context) error {
	var lastInitErr error
	var idx int
	var t utils.Transformer
	for idx, t = range tp.table.Transformers {
		if err := t.Init(ctx); err != nil {
			lastInitErr = err
			log.Warn().Err(err).Msg("error initializing transformer")
		}
	}

	if lastInitErr != nil {
		lastInitialized := idx
		for _, t = range tp.table.Transformers[:lastInitialized] {
			if err := t.Done(ctx); err != nil {
				log.Warn().Err(err).Msg("error terminating previously initialized transformer")
			}
		}
	}
	if lastInitErr != nil {
		return fmt.Errorf("unable to initialize transformer: %w", lastInitErr)
	}
	if tp.isAsync {
		for _, w := range tp.transformationWindows {
			w.Init()
		}
	}

	return nil
}

func (tp *TransformationPipeline) TransformSync(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	for _, t := range tp.table.Transformers {
		_, err = t.Transform(ctx, r)
		if err != nil {
			return nil, NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
		}
	}
	return r, nil
}

func (tp *TransformationPipeline) TransformAsync(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	for _, w := range tp.transformationWindows {
		_, err = w.Transform(ctx, r)
		if err != nil {
			return nil, NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
		}
	}
	return r, nil
}

func (tp *TransformationPipeline) Dump(ctx context.Context, data []byte) (err error) {
	tp.line++
	if err = tp.row.Decode(data[:len(data)-1]); err != nil {
		return fmt.Errorf("error decoding copy line: %w", err)
	}
	tp.record.SetRow(tp.row)

	_, err = tp.Transform(ctx, tp.record)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
	}
	rowDriver, err := tp.record.Encode()
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error enocding to RowDriver: %w", err))
	}
	res, err := rowDriver.Encode()
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error encoding RowDriver to []byte: %w", err))
	}

	_, err = tp.w.Write(res)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error writing dumped data: %w", err))
	}
	_, err = tp.w.Write(endOfLineSeq)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error writing dumped data: %w", err))
	}
	return nil
}

func (tp *TransformationPipeline) CompleteDump() (err error) {
	res := make([]byte, 0, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = tp.w.Write(res)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}

func (tp *TransformationPipeline) Done(ctx context.Context) error {
	var lastErr error
	for _, t := range tp.table.Transformers {
		if err := t.Done(ctx); err != nil {
			lastErr = err
			log.Warn().Err(err).Msg("error terminating initialized transformer")
		}
	}
	if tp.isAsync {
		for _, w := range tp.transformationWindows {
			w.Done()
		}
	}

	if lastErr != nil {
		return fmt.Errorf("error terminating initialized transformer: %w", lastErr)
	}
	return nil
}
