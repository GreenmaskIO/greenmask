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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestCmdTransformer_default_driver_params_not_mutated(t *testing.T) {
	expected := defaultRowDriverParams
	_ = runCmdTransformer(t, []string{"id", "data"}, "1\tx", map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("cat"),
		"driver":     toolkit.ParamsValue(`{"name":"csv","csv_attributes_format":"direct"}`),
	})
	require.Equal(t, expected, defaultRowDriverParams)
}

func TestCmdTransformer_skip_on_null_input_only_flagged_columns(t *testing.T) {
	r := runCmdTransformer(t, []string{"id", "data", "first_name"}, "1\tx\t\\N", map[string]toolkit.ParamsValue{
		"columns": toolkit.ParamsValue(
			`[{"name":"data","skip_on_null_input":true},{"name":"first_name","not_affected":true}]`,
		),
		"executable":        toolkit.ParamsValue("awk"),
		"args":              toolkit.ParamsValue(`["{print \"Y,\"; fflush()}"]`),
		"skip_on_behaviour": toolkit.ParamsValue("any"),
	})
	v, err := r.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.Equal(t, "Y", string(v.Data))
}

func TestCmdTransformer_Done_after_failed_Init(t *testing.T) {
	driver, _ := getDriverAndRecordByColumns([]string{"id", "data"}, "1\tx")
	tc, warnings, err := CmdTransformerDefinition.Instance(context.Background(), driver, map[string]toolkit.ParamsValue{
		"columns":    toolkit.ParamsValue(`[{"name":"data"}]`),
		"executable": toolkit.ParamsValue("/nonexistent/executable"),
	}, nil, "", false)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Error(t, tc.Transformer.Init(context.Background()))
	require.NotPanics(t, func() {
		_ = tc.Transformer.Done(context.Background())
	})
}

func TestCmdTransformer_whole_tuple_affected_columns(t *testing.T) {
	driver, _ := getDriverAndRecordByColumns([]string{"id", "data"}, "1\tx")
	tc, warnings, err := CmdTransformerDefinition.Instance(context.Background(), driver, map[string]toolkit.ParamsValue{
		"executable": toolkit.ParamsValue("cat"),
	}, nil, "", false)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.Equal(t, map[int]string{0: "id", 1: "data"}, tc.Transformer.GetAffectedColumns())
}
