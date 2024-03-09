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

package transformers_new

//import (
//	"context"
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//	"github.com/stretchr/testify/require"
//
//	"github.com/greenmaskio/greenmask/pkg/toolkit"
//)
//
//func TestRandomBoolTransformer_Transform(t *testing.T) {
//
//	tests := []struct {
//		name       string
//		params     map[string]toolkit.ParamsValue
//		columnName string
//		original   string
//		isNull     bool
//	}{
//		{
//			name:       "common",
//			original:   "t",
//			columnName: "col_bool",
//			params:     map[string]toolkit.ParamsValue{},
//		},
//		{
//			name:       "keep_null false and NULL seq",
//			original:   "\\N",
//			columnName: "col_bool",
//			params: map[string]toolkit.ParamsValue{
//				"keep_null": toolkit.ParamsValue("false"),
//			},
//		},
//		{
//			name:       "keep_null true and NULL seq",
//			original:   "\\N",
//			columnName: "col_bool",
//			params: map[string]toolkit.ParamsValue{
//				"keep_null": toolkit.ParamsValue("true"),
//			},
//			isNull: true,
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			tt.params["column"] = toolkit.ParamsValue(tt.columnName)
//			driver, record := getDriverAndRecord(tt.columnName, tt.original)
//			transformerCtx, warnings, err := RandomBoolTransformerDefinition.Instance(
//				context.Background(),
//				driver,
//				tt.params,
//				nil,
//			)
//			require.NoError(t, err)
//			require.Empty(t, warnings)
//
//			r, err := transformerCtx.Transformer.Transform(
//				context.Background(),
//				record,
//			)
//			require.NoError(t, err)
//
//			val, err := r.GetColumnValueByName(tt.columnName)
//			require.NoError(t, err)
//			require.Equal(t, tt.isNull, val.IsNull)
//			if !tt.isNull {
//				assert.IsType(t, val.Value, true)
//			}
//		})
//	}
//}
