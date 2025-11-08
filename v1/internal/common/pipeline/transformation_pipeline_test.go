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

package pipeline

import (
	"bytes"
	"context"
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
