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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestCmdTransformer_csv_null(t *testing.T) {
	r := runCmdTransformer(t, []string{"id", "data", "first_name"}, "1\t\\N\tbob", map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"},{"name":"first_name"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"csv"}`),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.True(t, v.IsNull)
	v, err = r.GetRawColumnValueByName("first_name")
	require.NoError(t, err)
	require.Equal(t, "bob", string(v.Data))
}

func TestCmdTransformer_csv_single_empty_column(t *testing.T) {
	r := runCmdTransformer(t, []string{"id", "data"}, "1\t", map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"csv"}`),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.False(t, v.IsNull)
	require.Empty(t, v.Data)
}

func TestCmdTransformer_text_null(t *testing.T) {
	r := runCmdTransformer(t, []string{"id", "data"}, "1\t\\N", map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"text"}`),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.True(t, v.IsNull)
}

func TestCmdTransformer_text_long_value(t *testing.T) {
	long := strings.Repeat("a", 10000)
	r := runCmdTransformer(t, []string{"id", "data"}, "1\t"+long, map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"text"}`),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.Equal(t, long, string(v.Data))
}

func TestCmdTransformer_text_new_line_in_value(t *testing.T) {
	driver, record := getDriverAndRecordByColumns([]string{"id", "data"}, "1\tx")
	require.NoError(t, record.SetRawColumnValueByName("data", toolkit.NewRawValue([]byte("a\nb"), false)))
	tc, warnings, err := CmdTransformerDefinition.Instance(context.Background(), driver, map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"text"}`),
	}, nil, "", false)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.NoError(t, tc.Transformer.Init(context.Background()))
	defer func() {
		_ = tc.Transformer.Done(context.Background())
	}()
	_, err = tc.Transformer.Transform(context.Background(), record)
	require.ErrorContains(t, err, "new line")
}

func TestCmdTransformer_json_null_without_data(t *testing.T) {
	r := runCmdTransformer(t, []string{"id", "data"}, "1\tx", map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("awk"),
		"args":       toolkit.ParamsValue(`["{print \"{\\\"data\\\":{\\\"n\\\":true}}\"; fflush()}"]`),
		"driver":     toolkit.ParamsValue(`{"name":"json"}`),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.True(t, v.IsNull)
}
