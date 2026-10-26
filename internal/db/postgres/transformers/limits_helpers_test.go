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

package transformers

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func newTestTransformer(
	t *testing.T, name, column string, rec map[string]*toolkit.RawValue,
	params map[string]toolkit.ParamsValue, dynamicParams map[string]*toolkit.DynamicParamValue,
) (*utils.TransformerContext, *toolkit.Record, toolkit.ValidationWarnings) {
	t.Helper()
	driver, record := toolkit.GetDriverAndRecord(rec)
	params["column"] = toolkit.ParamsValue(column)
	def, ok := utils.DefaultTransformerRegistry.Get(name)
	require.True(t, ok)
	tr, warns, err := def.Instance(context.Background(), driver, params, dynamicParams, "", false)
	require.NoError(t, err)
	return tr, record, warns
}

// transformRows transforms the record n times and returns the raw values of the column.
func transformRows(
	t *testing.T, name, column string, rec map[string]*toolkit.RawValue,
	params map[string]toolkit.ParamsValue, dynamicParams map[string]*toolkit.DynamicParamValue, n int,
) []string {
	t.Helper()
	tr, record, warns := newTestTransformer(t, name, column, rec, params, dynamicParams)
	for _, w := range warns {
		t.Logf("warning: %s %+v", w.Msg, w.Meta)
	}
	require.Empty(t, warns)
	require.NoError(t, tr.Transformer.Init(context.Background()))
	for _, dp := range tr.DynamicParameters {
		dp.SetRecord(record)
	}
	idx := -1
	for i, c := range record.Driver.Table.Columns {
		if c.Name == column {
			idx = i
		}
	}
	require.NotEqual(t, -1, idx)
	res := make([]string, 0, n)
	for i := 0; i < n; i++ {
		row := make(toolkit.RawRecord)
		for ci, c := range record.Driver.Table.Columns {
			v := *rec[c.Name]
			row[ci] = &v
		}
		record.SetRow(&row)
		r, err := tr.Transformer.Transform(context.Background(), record)
		require.NoError(t, err)
		v, err := r.GetRawColumnValueByIdx(idx)
		require.NoError(t, err)
		require.False(t, v.IsNull)
		res = append(res, string(v.Data))
	}
	return res
}

func rawValues(kv ...string) map[string]*toolkit.RawValue {
	res := make(map[string]*toolkit.RawValue)
	for i := 0; i < len(kv); i += 2 {
		res[kv[i]] = toolkit.NewRawValue([]byte(kv[i+1]), false)
	}
	return res
}

func paramValues(kv ...string) map[string]toolkit.ParamsValue {
	res := make(map[string]toolkit.ParamsValue)
	for i := 0; i < len(kv); i += 2 {
		res[kv[i]] = toolkit.ParamsValue(kv[i+1])
	}
	return res
}

func parseFloats(t *testing.T, values []string) []float64 {
	t.Helper()
	res := make([]float64, 0, len(values))
	for _, v := range values {
		f, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		res = append(res, f)
	}
	return res
}

func requireAllInRange(t *testing.T, values []float64, minValue, maxValue float64) {
	t.Helper()
	for _, v := range values {
		require.GreaterOrEqual(t, v, minValue)
		require.LessOrEqual(t, v, maxValue)
	}
}
