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

package transformers2

import (
	"context"
	"reflect"
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

func TestReplaceNewReplaceTransformer(t *testing.T) {
	t.Run("success static and no validate", func(t *testing.T) {
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
		valueParameter := mocks.NewParametrizerMock()
		keepNullParameter := mocks.NewParametrizerMock()

		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(column.Name, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", column.Name).Return(&column, true)

		// Validate parameter calls
		validateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		// Value parameter calls
		valueParameter.On("IsDynamic").
			Return(false)
		valueParameter.On("RawValue").
			Return(commonmodels.ParamsValue("123"), nil)

		// Keep null parameter calls
		keepNullParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		tr, err := NewReplaceTransformer(ctx, vc, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*ReplaceTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.keepNull, true)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.validate, false)
		assert.Equal(t, replaceTransformer.columnOIDToValidate, column.TypeOID)

		expectedTransformationFunc := reflect.ValueOf(replaceTransformer.transformStatic).Pointer()
		actualTransformationFunc := reflect.ValueOf(replaceTransformer.transform).Pointer()

		assert.Equal(t, expectedTransformationFunc, actualTransformationFunc,
			"transform should be transformStatic",
		)

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		valueParameter.AssertExpectations(t)
		keepNullParameter.AssertExpectations(t)
	})

	t.Run("success static and validate and valid", func(t *testing.T) {
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
		valueParameter := mocks.NewParametrizerMock()
		keepNullParameter := mocks.NewParametrizerMock()

		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(column.Name, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", column.Name).Return(&column, true)

		// Validate parameter calls
		validateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// Value parameter calls
		valueParameter.On("IsDynamic").
			Return(false)
		valueParameter.On("RawValue").
			Return(commonmodels.ParamsValue("123"), nil)
		tableDriver.On("DecodeValueByTypeOid", column.TypeOID, []byte("123")).
			Return(&commonmodels.ColumnValue{
				Value:  int64(123),
				IsNull: false,
			}, nil)

		// Keep null parameter calls
		keepNullParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		tr, err := NewReplaceTransformer(ctx, vc, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*ReplaceTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.keepNull, true)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.validate, true)
		assert.Equal(t, replaceTransformer.columnOIDToValidate, column.TypeOID)

		expectedTransformationFunc := reflect.ValueOf(replaceTransformer.transformStatic).Pointer()
		actualTransformationFunc := reflect.ValueOf(replaceTransformer.transform).Pointer()

		assert.Equal(t, expectedTransformationFunc, actualTransformationFunc,
			"transform should be transformStatic",
		)

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		valueParameter.AssertExpectations(t)
		keepNullParameter.AssertExpectations(t)
	})

	t.Run("failure static and validate and invalid", func(t *testing.T) {
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
		valueParameter := mocks.NewParametrizerMock()
		keepNullParameter := mocks.NewParametrizerMock()

		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(column.Name, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", column.Name).Return(&column, true)

		// Validate parameter calls
		validateParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// Value parameter calls
		valueParameter.On("IsDynamic").
			Return(false)
		valueParameter.On("RawValue").
			Return(commonmodels.ParamsValue("abc"), nil)
		tableDriver.On("DecodeValueByTypeOid", column.TypeOID, []byte("abc")).
			Return(nil, assert.AnError)
		valueParameter.On("Name").
			Return("value")

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		_, err := NewReplaceTransformer(ctx, vc, tableDriver, parameters)
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "error validating parameter value")

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		valueParameter.AssertExpectations(t)
		keepNullParameter.AssertExpectations(t)
	})
}

//func TestReplaceTransformer_Transform_with_raw_value(t *testing.T) {
//	type result struct {
//		isNull bool
//		value  string
//	}
//
//	tests := []struct {
//		name       string
//		params     map[string]commonmodels.ParamsValue
//		columnName string
//		original   string
//		result     result
//	}{
//		{
//			name:       "common",
//			original:   `{}`,
//			columnName: "doc",
//			params: map[string]commonmodels.ParamsValue{
//				"value": commonmodels.ParamsValue(`{"test": 1234}`),
//			},
//			result: result{
//				isNull: false,
//				value:  `{"test": 1234}`,
//			},
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			driver, record := transformers.getDriverAndRecord(tt.columnName, tt.original)
//			tt.params["column"] = commonmodels.ParamsValue(tt.columnName)
//			transformerCtx, warnings, err := ReplaceTransformerDefinition.Instance(
//				context.Background(),
//				driver,
//				tt.params,
//				nil,
//				"",
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
//			attVal, err := r.GetColumnValueByName(tt.columnName)
//			require.Equal(t, tt.result.isNull, attVal.IsNull)
//			require.NoError(t, err)
//			encoded, err := r.Encode()
//			require.NoError(t, err)
//			res, err := encoded.Encode()
//			require.NoError(t, err)
//			require.JSONEq(t, tt.result.value, string(res))
//		})
//
//	}
//}
//
//func TestReplaceTransformer_Transform(t *testing.T) {
//	tests := []struct {
//		name       string
//		params     map[string]commonmodels.ParamsValue
//		columnName string
//		original   *commonmodels.ColumnRawValue
//	}{
//		{
//			name:       "common",
//			original:   commonmodels.NewColumnRawValue([]byte("1"), false),
//			columnName: "id",
//			params: map[string]commonmodels.ParamsValue{
//				"value": commonmodels.ParamsValue("123"),
//			},
//		},
//		{
//			name:       "keep_null false and NULL seq",
//			original:   commonmodels.NewColumnRawValue([]byte("1"), false),
//			columnName: "id",
//			params: map[string]commonmodels.ParamsValue{
//				"value":     commonmodels.ParamsValue("123"),
//				"keep_null": commonmodels.ParamsValue("false"),
//			},
//		},
//		{
//			name:       "keep_null true and NULL seq",
//			original:   commonmodels.NewColumnRawValue([]byte("1"), false),
//			columnName: "id",
//			params: map[string]commonmodels.ParamsValue{
//				"value":     commonmodels.ParamsValue("123"),
//				"keep_null": commonmodels.ParamsValue("true"),
//			},
//		},
//	}
//
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			vc := validationcollector.NewCollector()
//			ctx := context.Background()
//			tableDriver := mocks.NewTableDriverMock()
//			columnParameter := mocks.NewParametrizerMock()
//			columnParameter := mocks.NewParametrizerMock()
//
//			NewReplaceTransformer(ctx, vc, tableDriver)
//		})
//	}
//
//	t.Run("replace with null value", func(t *testing.T) {
//		vc := validationcollector.NewCollector()
//		ctx := context.Background()
//		tableDriver := mocks.NewTableDriverMock()
//		columnParameter := mocks.NewParametrizerMock()
//		valueParameter := mocks.NewParametrizerMock()
//		validateParameter := mocks.NewParametrizerMock()
//		keepNullParameter := mocks.NewParametrizerMock()
//
//		parameters := map[string]commonparameters.Parameterizer{
//			"column":    columnParameter,
//			"value":     valueParameter,
//			"validate":  validateParameter,
//			"keep_null": keepNullParameter,
//		}
//
//		NewReplaceTransformer(ctx, vc, tableDriver)
//	})
//}
//
//func TestReplaceTransformer_Transform_with_validation_error(t *testing.T) {
//
//	original := "doc"
//	columnName := "doc"
//	params := map[string]commonmodels.ParamsValue{
//		"column":   commonmodels.ParamsValue(columnName),
//		"value":    commonmodels.ParamsValue(`{"test": 1a234}`),
//		"validate": commonmodels.ParamsValue("true"),
//	}
//	driver, _ := transformers.getDriverAndRecord(columnName, original)
//
//	_, warnings, err := ReplaceTransformerDefinition.Instance(
//		context.Background(),
//		driver,
//		params,
//		nil,
//		"",
//	)
//	require.NoError(t, err)
//	assert.NotEmpty(t, warnings)
//	assert.Equal(t, warnings[0].Severity, toolkit.ErrorValidationSeverity)
//}
