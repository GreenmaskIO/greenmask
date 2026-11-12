package dumpers

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestTransformationPipeline_Dump(t *testing.T) {
	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	tableCond := ""
	table := getTable(tableCond)
	ctx := context.Background()
	eg, gtx := errgroup.WithContext(ctx)
	driver := getDriver(table.Table)
	table.Driver = driver
	when, warns := toolkit.NewWhenCond("", driver, nil)
	require.Empty(t, warns)
	tt := &testTransformer{}
	tc := &utils.TransformerContext{
		Transformer: tt,
		When:        when,
	}
	table.TransformersContext = []*utils.TransformerContext{tc}

	buf := bytes.NewBuffer(nil)

	pipeline, err := NewTransformationPipeline(gtx, eg, table, buf)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(termCtx))
	data := []byte("1\t2023-08-27 00:00:00.000000")
	err = pipeline.Dump(ctx, data)
	require.NoError(t, err)
	require.NoError(t, pipeline.Done(termCtx))
	require.NoError(t, pipeline.CompleteDump())
	require.Equal(t, tt.callsCount, 1)
	require.Equal(t, buf.String(), "2\t2023-08-27 00:00:00.00000\n\\.\n\n")
}

func TestTransformationPipeline_Dump_with_transformer_cond(t *testing.T) {
	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	tableCond := ""
	table := getTable(tableCond)
	ctx := context.Background()
	eg, gtx := errgroup.WithContext(ctx)
	driver := getDriver(table.Table)
	table.Driver = driver
	when, warns := toolkit.NewWhenCond("record.id != 1", driver, make(map[string]any))
	require.Empty(t, warns)
	tt := &testTransformer{}
	tc := &utils.TransformerContext{
		Transformer: tt,
		When:        when,
	}
	table.TransformersContext = []*utils.TransformerContext{tc}

	buf := bytes.NewBuffer(nil)

	pipeline, err := NewTransformationPipeline(gtx, eg, table, buf)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(termCtx))
	data := []byte("1\t2023-08-27 00:00:00.000000")
	err = pipeline.Dump(ctx, data)
	require.NoError(t, err)
	require.NoError(t, pipeline.Done(termCtx))
	require.NoError(t, pipeline.CompleteDump())
	require.Equal(t, tt.callsCount, 0)
	require.Equal(t, buf.String(), "1\t2023-08-27 00:00:00.00000\n\\.\n\n")
}

func TestTransformationPipeline_Dump_with_table_cond(t *testing.T) {
	termCtx, termCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer termCancel()
	tableCond := "record.id != 1"
	table := getTable(tableCond)
	ctx := context.Background()
	eg, gtx := errgroup.WithContext(ctx)
	driver := getDriver(table.Table)
	table.Driver = driver
	when, warns := toolkit.NewWhenCond("", driver, make(map[string]any))
	require.Empty(t, warns)
	tt := &testTransformer{}
	tc := &utils.TransformerContext{
		Transformer: tt,
		When:        when,
	}
	table.TransformersContext = []*utils.TransformerContext{tc}

	buf := bytes.NewBuffer(nil)

	pipeline, err := NewTransformationPipeline(gtx, eg, table, buf)
	require.NoError(t, err)
	require.NoError(t, pipeline.Init(termCtx))
	data := []byte("1\t2023-08-27 00:00:00.000000")
	err = pipeline.Dump(ctx, data)
	require.NoError(t, err)
	require.NoError(t, pipeline.Done(termCtx))
	require.NoError(t, pipeline.CompleteDump())
	require.Equal(t, tt.callsCount, 0)
	require.Equal(t, buf.String(), "1\t2023-08-27 00:00:00.00000\n\\.\n\n")
}

type initTrackingTransformer struct {
	testTransformer
	initErr   error
	doneCalls int
}

func (it *initTrackingTransformer) Init(ctx context.Context) error { return it.initErr }

func (it *initTrackingTransformer) Done(ctx context.Context) error {
	it.doneCalls++
	return nil
}

func TestTransformationPipeline_Init_terminates_all_on_failure(t *testing.T) {
	table := getTable("")
	when, warns := toolkit.NewWhenCond("", table.Driver, nil)
	require.Empty(t, warns)
	first := &initTrackingTransformer{}
	failed := &initTrackingTransformer{initErr: errors.New("init failed")}
	last := &initTrackingTransformer{}
	table.TransformersContext = []*utils.TransformerContext{
		{Transformer: first, When: when},
		{Transformer: failed, When: when},
		{Transformer: last, When: when},
	}

	eg, gtx := errgroup.WithContext(context.Background())
	pipeline, err := NewTransformationPipeline(gtx, eg, table, bytes.NewBuffer(nil))
	require.NoError(t, err)
	require.Error(t, pipeline.Init(context.Background()))
	require.Equal(t, 1, first.doneCalls)
	require.Equal(t, 1, failed.doneCalls)
	require.Equal(t, 1, last.doneCalls)
}
