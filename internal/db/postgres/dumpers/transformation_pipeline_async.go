package dumpers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
)

type TransformationWindow struct {
	affectedColumns map[string]struct{}
	transformers    []utils.Transformer
	done            chan struct{}
	wg              *sync.WaitGroup
}

func NewTransformationWindow() *TransformationWindow {
	return &TransformationWindow{
		affectedColumns: map[string]struct{}{},
		done:            make(chan struct{}, 1),
		wg:              &sync.WaitGroup{},
	}
}

func (tw *TransformationWindow) TryAdd(t utils.Transformer) bool {
	affectedColumn := t.GetAffectedColumns()
	if affectedColumn == nil || len(affectedColumn) == 0 {
		if len(tw.transformers) == 0 {
			tw.transformers = append(tw.transformers, t)
			return true
		}
		return false
	}

	for _, name := range affectedColumn {
		if _, ok := tw.affectedColumns[name]; ok {
			return false
		}
	}
	for _, name := range affectedColumn {
		tw.affectedColumns[name] = struct{}{}
	}
	tw.transformers = append(tw.transformers, t)
	return true
}

func (tw *TransformationWindow) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var errGlobal error
	for _, t := range tw.transformers {
		tw.wg.Add(1)
		func(t utils.Transformer) {
			go func() {
				_, err := t.Transform(ctx, r)
				if err != nil {
					errGlobal = err
				}
				tw.wg.Done()
			}()
		}(t)
	}

	tw.wg.Wait()
	if errGlobal != nil {
		return nil, errGlobal
	}

	return r, nil
}

type TransformationPipelineAsync struct {
	table                 *dump.Table
	buf                   *bytes.Buffer
	w                     io.Writer
	line                  int64
	row                   *pgcopy.Row
	transformationWindows []*TransformationWindow
}

func NewTransformationPipelineAsync(ctx context.Context, table *dump.Table, w io.Writer) (*TransformationPipelineAsync, error) {

	var tws []*TransformationWindow

	tw := NewTransformationWindow()
	tws = append(tws, tw)
	for _, t := range table.Transformers {
		if !tw.TryAdd(t) {
			tw = NewTransformationWindow()
			tws = append(tws, tw)
			tw.TryAdd(t)
		}
	}

	return &TransformationPipelineAsync{
		table:                 table,
		buf:                   bytes.NewBuffer(nil),
		w:                     w,
		row:                   pgcopy.NewRow(len(table.Columns)),
		transformationWindows: tws,
	}, nil
}

func (wt *TransformationPipelineAsync) Init(ctx context.Context) error {
	var lastInitErr error
	var idx int
	var t utils.Transformer
	for idx, t = range wt.table.Transformers {
		if err := t.Init(ctx); err != nil {
			lastInitErr = err
			log.Warn().Err(err).Msg("error initializing transformer")
		}
	}

	if lastInitErr != nil {
		lastInitialized := idx
		for _, t = range wt.table.Transformers[:lastInitialized] {
			if err := t.Done(ctx); err != nil {
				log.Warn().Err(err).Msg("error terminating previously initialized transformer")
			}
		}
	}
	if lastInitErr != nil {
		return fmt.Errorf("unable to initialize transformer: %w", lastInitErr)
	}

	return nil
}

func (wt *TransformationPipelineAsync) Dump(ctx context.Context, data []byte) (err error) {
	wt.line++
	_, err = wt.buf.Write(data)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
	}
	wt.row.Parse(data[:len(data)-1])
	record := toolkit.NewRecord(wt.table.Driver, wt.row)
	for _, w := range wt.transformationWindows {
		_, err = w.Transform(ctx, record)
		if err != nil {
			return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, err)
		}
	}
	rowDriver, err := record.Encode()
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error enocding to RowDriver: %w", err))
	}
	res, err := rowDriver.Encode()
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error RowDriver to []byte: %w", err))
	}
	res = append(res, '\n')
	_, err = wt.w.Write(res)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error writing dumped data: %w", err))
	}
	return nil
}

func (wt *TransformationPipelineAsync) CompleteDump() (err error) {
	res := make([]byte, 0, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = wt.w.Write(res)
	if err != nil {
		return NewDumpError(wt.table.Schema, wt.table.Name, wt.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}

func (wt *TransformationPipelineAsync) Done(ctx context.Context) error {
	var lastErr error
	for _, t := range wt.table.Transformers {
		if err := t.Done(ctx); err != nil {
			lastErr = err
			log.Warn().Err(err).Msg("error terminating initialized transformer")
		}
	}
	for _, w := range wt.transformationWindows {
		close(w.done)
	}

	if lastErr != nil {
		return fmt.Errorf("error terminating initialized transformer: %w", lastErr)
	}
	return nil
}
