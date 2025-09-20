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

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestNewDictTransformer(t *testing.T) {
	t.Run("success no needValidate no keep null", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := context.Background()
		column := commonmodels.Column{
			Idx:      1,
			Name:     "id",
			TypeName: "int",
			TypeOID:  2,
		}
		tableDriver := mocks.NewTableDriverMock()
		columnParameter := mocks.NewParametrizerMock()
		validateParameter := mocks.NewParametrizerMock()
		valuesParameter := mocks.NewParametrizerMock()
		defaultValueParameter := mocks.NewParametrizerMock()
		failNotMatchedParameter := mocks.NewParametrizerMock()

		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(column.Name, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", column.Name).Return(&column, nil)

		// Validate parameter calls
		validateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		// Value parameter calls
		values := map[string]string{
			"1": "2",
		}
		valuesParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(values, dest))
			}).Return(nil)

		// default value parameter calls
		defaultValueParameter.On("IsEmpty").
			Return(true, nil)

		failNotMatchedParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":           columnParameter,
			"values":           valuesParameter,
			"needValidate":     validateParameter,
			"default":          defaultValueParameter,
			"fail_not_matched": failNotMatchedParameter,
		}

		tr, err := NewDictTransformer(ctx, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*DictTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.needValidate, false)
		assert.Equal(t, replaceTransformer.failNotMatched, true)
		require.Len(t, replaceTransformer.dict, 1)
		v, ok := replaceTransformer.dict["1"]
		require.True(t, ok)
		require.NotNil(t, v)
		assert.Equal(t, v.Data, []byte("2"))
		assert.False(t, v.IsNull)

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		valuesParameter.AssertExpectations(t)
		defaultValueParameter.AssertExpectations(t)
		failNotMatchedParameter.AssertExpectations(t)
	})
}

//func TestDictTransformer_Transform(t *testing.T) {
//	t.Run("fail", func(t *testing.T) {
//		original := "2023-11-10"
//		expected := "2023-01-01"
//
//		params := map[string]toolkit.ParamsValue{
//			"column":           toolkit.ParamsValue("date_date"),
//			"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01", "2023-11-11": "2023-01-02"}`),
//			"fail_not_matched": toolkit.ParamsValue(`true`),
//			"needValidate":         toolkit.ParamsValue(`true`),
//		}
//
//		driver, record := getDriverAndRecord(string(params["column"]), original)
//		transformerCtx, warnings, err := DictTransformerDefinition.Instance(
//			context.Background(),
//			driver, params,
//			nil,
//			"",
//		)
//		require.NoError(t, err)
//		require.Empty(t, warnings)
//		r, err := transformerCtx.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//
//		res, err := r.GetRawColumnValueByName(string(params["column"]))
//		require.NoError(t, err)
//		assert.False(t, res.IsNull)
//		require.Equal(t, expected, string(res.Data))
//
//		original = "2023-11-11"
//		expected = "2023-01-02"
//		_, record = getDriverAndRecord(string(params["column"]), original)
//		r, err = transformerCtx.Transformer.Transform(
//			context.Background(),
//			record,
//		)
//		require.NoError(t, err)
//		res, err = r.GetRawColumnValueByName(string(params["column"]))
//		require.NoError(t, err)
//		assert.False(t, res.IsNull)
//		require.Equal(t, expected, string(res.Data))
//	})
//}
//
//func TestDictTransformer_Transform_validation_error(t *testing.T) {
//
//	original := "2023-11-10"
//
//	params := map[string]toolkit.ParamsValue{
//		"column":   toolkit.ParamsValue("date_date"),
//		"values":   toolkit.ParamsValue(`{"2023-11-10": "value_error"}`),
//		"needValidate": toolkit.ParamsValue(`true`),
//	}
//
//	driver, _ := getDriverAndRecord(string(params["column"]), original)
//	_, warnings, err := DictTransformerDefinition.Instance(
//		context.Background(),
//		driver, params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.NotEmpty(t, warnings)
//
//	params = map[string]toolkit.ParamsValue{
//		"column":   toolkit.ParamsValue("date_date"),
//		"values":   toolkit.ParamsValue(`{"2023-11-14": "2023-11-10"}`),
//		"default":  toolkit.ParamsValue(`asdnakmsd`),
//		"needValidate": toolkit.ParamsValue(`true`),
//	}
//
//	driver, _ = getDriverAndRecord(string(params["column"]), original)
//	_, warnings, err = DictTransformerDefinition.Instance(
//		context.Background(),
//		driver, params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.NotEmpty(t, warnings)
//}
//
//func TestDictTransformer_Transform_error_not_matched(t *testing.T) {
//	original := "2022-10-10"
//
//	params := map[string]toolkit.ParamsValue{
//		"column":           toolkit.ParamsValue("date_date"),
//		"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01"}`),
//		"fail_not_matched": toolkit.ParamsValue(`true`),
//		"needValidate":         toolkit.ParamsValue(`true`),
//	}
//
//	driver, record := getDriverAndRecord(string(params["column"]), original)
//	transformer, warnings, err := DictTransformerDefinition.Instance(
//		context.Background(),
//		driver, params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//	_, err = transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.Error(t, err)
//	require.ErrorContains(t, err, `unable to match value for`)
//}
//
//func TestDictTransformer_Transform_use_default(t *testing.T) {
//	original := "2022-10-10"
//	expected := "2024-11-10"
//
//	params := map[string]toolkit.ParamsValue{
//		"column":           toolkit.ParamsValue("date_date"),
//		"values":           toolkit.ParamsValue(`{"2023-11-10": "2023-01-01"}`),
//		"fail_not_matched": toolkit.ParamsValue(`true`),
//		"needValidate":         toolkit.ParamsValue(`true`),
//		"default":          toolkit.ParamsValue(expected),
//	}
//
//	driver, record := getDriverAndRecord(string(params["column"]), original)
//	transformer, warnings, err := DictTransformerDefinition.Instance(
//		context.Background(),
//		driver, params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//	r, err := transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.NoError(t, err)
//	res, err := r.GetRawColumnValueByName(string(params["column"]))
//	require.NoError(t, err)
//	assert.False(t, res.IsNull)
//	require.Equal(t, expected, string(res.Data))
//}
//
//func TestDictTransformer_Transform_with_int_values(t *testing.T) {
//
//	original := "1"
//	expected := "2"
//
//	params := map[string]toolkit.ParamsValue{
//		"column":           toolkit.ParamsValue("id"),
//		"values":           toolkit.ParamsValue(`{"1": "2", "3": "4"}`),
//		"fail_not_matched": toolkit.ParamsValue(`true`),
//		"needValidate":         toolkit.ParamsValue(`true`),
//	}
//
//	driver, record := getDriverAndRecord(string(params["column"]), original)
//	transformer, warnings, err := DictTransformerDefinition.Instance(
//		context.Background(),
//		driver, params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	require.Empty(t, warnings)
//	r, err := transformer.Transformer.Transform(
//		context.Background(),
//		record,
//	)
//	require.NoError(t, err)
//
//	res, err := r.GetRawColumnValueByName(string(params["column"]))
//	require.NoError(t, err)
//	assert.False(t, res.IsNull)
//	require.Equal(t, expected, string(res.Data))
//
//}
