package dumpers

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type TransformationFunc func(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error)

type TransformationPipelineAsync struct {
	table                 *dump.Table
	buf                   *bytes.Buffer
	w                     io.Writer
	line                  int64
	row                   *pgcopy.Row
	transformationWindows []*TransformationWindow
	Transform             TransformationFunc
	isAsync               bool
}

func NewTransformationPipelineAsync(ctx context.Context, eg *errgroup.Group, table *dump.Table, w io.Writer) (*TransformationPipelineAsync, error) {

	var tws []*TransformationWindow
	var isAsync bool

	if table.HasCustomTransformer() && len(table.Transformers) > 1 {
		isAsync = true
		tw := NewTransformationWindow(ctx, eg)
		tws = append(tws, tw)
		for _, t := range table.Transformers {
			if !tw.TryAdd(t) {
				tw = NewTransformationWindow(ctx, eg)
				tws = append(tws, tw)
				tw.TryAdd(t)
			}

		}
	}

	tp := &TransformationPipelineAsync{
		table:                 table,
		buf:                   bytes.NewBuffer(nil),
		w:                     w,
		row:                   pgcopy.NewRow(len(table.Columns)),
		transformationWindows: tws,
		isAsync:               true,
	}

	var tf TransformationFunc = tp.TransformSync
	if isAsync {
		tf = tp.TransformAsync
	}
	tp.Transform = tf

	return tp, nil
}

func (tp *TransformationPipelineAsync) Init(ctx context.Context) error {
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

func (tp *TransformationPipelineAsync) TransformSync(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	for _, t := range tp.table.Transformers {
		_, err = t.Transform(ctx, r)
		if err != nil {
			return nil, NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
		}
	}
	return r, nil
}

func (tp *TransformationPipelineAsync) TransformAsync(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	var err error
	for _, w := range tp.transformationWindows {
		_, err = w.Transform(ctx, r)
		if err != nil {
			return nil, NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
		}
	}
	return r, nil
}

func (tp *TransformationPipelineAsync) Dump(ctx context.Context, data []byte) (err error) {
	tp.line++
	_, err = tp.buf.Write(data)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
	}
	tp.row.Parse(data[:len(data)-1])
	record := toolkit.NewRecord(tp.table.Driver, tp.row)

	_, err = tp.Transform(ctx, record)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, err)
	}
	rowDriver, err := record.Encode()
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error enocding to RowDriver: %w", err))
	}
	res, err := rowDriver.Encode()
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error encoding RowDriver to []byte: %w", err))
	}
	res = append(res, '\n')
	_, err = tp.w.Write(res)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error writing dumped data: %w", err))
	}
	return nil
}

func (tp *TransformationPipelineAsync) CompleteDump() (err error) {
	res := make([]byte, 0, 4)
	res = append(res, pgcopy.DefaultCopyTerminationSeq...)
	res = append(res, '\n', '\n')
	_, err = tp.w.Write(res)
	if err != nil {
		return NewDumpError(tp.table.Schema, tp.table.Name, tp.line, fmt.Errorf("error end of dump symbols: %w", err))
	}
	return nil
}

func (tp *TransformationPipelineAsync) Done(ctx context.Context) error {
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
