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

package parameters

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type recorderMock struct {
	mock.Mock
}

func (r *recorderMock) IsNullByColumnName(columName string) (bool, error) {
	args := r.Called(columName)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) IsNullByColumnIdx(columIdx int) (bool, error) {
	args := r.Called(columIdx)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) GetRawColumnValueByIdx(columnIdx int) (*commonmodels.ColumnRawValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnRawValue), nil
}

func (r *recorderMock) GetColumnValueByIdx(columnIdx int) (*commonmodels.ColumnValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnValue), nil
}

func (r *recorderMock) GetColumnValueByName(columnName string) (*commonmodels.ColumnValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnValue), nil
}

func (r *recorderMock) GetRawColumnValueByName(columnName string) (*commonmodels.ColumnRawValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*commonmodels.ColumnRawValue), nil
}

func (r *recorderMock) SetColumnValueByIdx(columnIdx int, v any) error {
	args := r.Called(columnIdx, v)
	return args.Error(0)
}

func (r *recorderMock) SetRawColumnValueByIdx(columnIdx int, value *commonmodels.ColumnRawValue) error {
	args := r.Called(columnIdx, value)
	return args.Error(0)
}

func (r *recorderMock) SetColumnValueByName(columnName string, v any) error {
	args := r.Called(columnName, v)
	return args.Error(0)
}

func (r *recorderMock) SetRawColumnValueByName(columnName string, value *commonmodels.ColumnRawValue) error {
	args := r.Called(columnName, value)
	return args.Error(0)
}

func (r *recorderMock) GetColumnByName(columnName string) (*commonmodels.Column, bool) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, false
	}
	return args.Get(0).(*commonmodels.Column), true
}

func (r *recorderMock) TableDriver() commonininterfaces.TableDriver {
	args := r.Called()
	return args.Get(0).(commonininterfaces.TableDriver)
}

func newRecorderMock() *recorderMock {
	return &recorderMock{}
}

type tableDriverMock struct {
	mock.Mock
}

func newTableDriverMock() *tableDriverMock {
	return &tableDriverMock{}
}

func (t *tableDriverMock) GetColumnIdxByName(name string) (int, error) {
	args := t.Called(name)
	return args.Int(0), args.Error(1)
}

func (t *tableDriverMock) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	args := t.Called(name, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *tableDriverMock) EncodeValueByTypeOid(oid commonmodels.VirtualOID, src any, buf []byte) ([]byte, error) {
	args := t.Called(oid, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *tableDriverMock) DecodeValueByTypeName(name string, src []byte) (any, error) {
	args := t.Called(name, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func (t *tableDriverMock) DecodeValueByTypeOid(oid commonmodels.VirtualOID, src []byte) (any, error) {
	args := t.Called(oid, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func (t *tableDriverMock) ScanValueByTypeName(name string, src []byte, dest any) error {
	args := t.Called(name, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *tableDriverMock) ScanValueByTypeOid(oid commonmodels.VirtualOID, src []byte, dest any) error {
	args := t.Called(oid, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *tableDriverMock) TypeExistsByName(name string) bool {
	args := t.Called(name)
	if args.Get(0) == nil {
		return false
	}
	exists, ok := args.Get(0).(bool)
	if !ok {
		panic(fmt.Sprintf("expected bool, got %T", args.Get(0)))
	}
	return exists
}

func (t *tableDriverMock) TypeExistsByOid(oid commonmodels.VirtualOID) bool {
	args := t.Called(oid)
	if args.Get(0) == nil {
		return false
	}
	exists, ok := args.Get(0).(bool)
	if !ok {
		panic(fmt.Sprintf("expected bool, got %T", args.Get(0)))
	}
	return exists
}

func (t *tableDriverMock) GetTypeOid(name string) (commonmodels.VirtualOID, error) {
	args := t.Called(name)
	if args.Get(0) == nil {
		return commonmodels.VirtualOID(0), args.Error(1)
	}
	oid, ok := args.Get(0).(commonmodels.VirtualOID)
	if !ok {
		panic(fmt.Sprintf("expected commonmodels.VirtualOID, got %T", args.Get(0)))
	}
	return oid, args.Error(1)
}

func (t *tableDriverMock) GetCanonicalTypeName(typeName string, typeOid commonmodels.VirtualOID) (string, error) {
	args := t.Called(typeName, typeOid)
	if args.Get(0) == nil {
		return "", args.Error(1)
	}
	canonicalTypeName, ok := args.Get(0).(string)
	if !ok {
		panic(fmt.Sprintf("expected string, got %T", args.Get(0)))
	}
	return canonicalTypeName, args.Error(1)
}

func (t *tableDriverMock) EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error) {
	args := t.Called(idx, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *tableDriverMock) EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error) {
	args := t.Called(name, src, buf)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	if buf != nil {
		return append(buf, args.Get(0).([]byte)...), nil
	}
	return args.Get(0).([]byte), args.Error(1)
}

func (t *tableDriverMock) ScanValueByColumnIdx(idx int, src []byte, dest any) error {
	args := t.Called(idx, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *tableDriverMock) ScanValueByColumnName(name string, src []byte, dest any) error {
	args := t.Called(name, src, dest)
	if args.Get(0) != nil {
		return args.Error(0)
	}
	return nil
}

func (t *tableDriverMock) DecodeValueByColumnIdx(idx int, src []byte) (any, error) {
	args := t.Called(idx, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	value, ok := args.Get(0).(any)
	if !ok {
		panic(fmt.Sprintf("expected any, got %T", args.Get(0)))
	}
	return value, args.Error(1)
}

func (t *tableDriverMock) DecodeValueByColumnName(name string, src []byte) (any, error) {
	args := t.Called(name, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	value, ok := args.Get(0).(any)
	if !ok {
		panic(fmt.Sprintf("expected any, got %T", args.Get(0)))
	}
	return value, args.Error(1)
}

func (t *tableDriverMock) GetColumnByName(name string) (*commonmodels.Column, error) {
	args := t.Called(name)
	if args.Get(0) == nil {
		return nil, nil
	}
	column, ok := args.Get(0).(*commonmodels.Column)
	if !ok {
		panic(fmt.Sprintf("expected *commonmodels.Column, got %T", args.Get(0)))
	}
	return column, args.Error(1)
}

func (t *tableDriverMock) Table() *commonmodels.Table {
	args := t.Called()
	if args.Get(0) == nil {
		return nil
	}
	table, ok := args.Get(0).(*commonmodels.Table)
	if !ok {
		panic(fmt.Sprintf("expected *commonmodels.Table, got %T", args.Get(0)))
	}
	return table
}

func TestDynamicParameter_Init(t *testing.T) {
	t.Run("error column param cannot be dynamic", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("text"),
			)
		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		parameter := NewDynamicParameter(columnDef, tableDriver)
		err := parameter.Init(vc, nil, commonmodels.DynamicParamValue{
			Column: "data",
		})
		assert.True(t, vc.IsFatal())
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, slices.ContainsFunc(vc.GetWarnings(), func(w *commonmodels.ValidationWarning) bool {
			return strings.Contains(w.Msg, "parameter does not support dynamic mode")
		}))
	})

	t.Run("linked column parameter and unsupported type", func(t *testing.T) {
		// First create a column parameter with supported INT types.
		// Then create a dynamic parameter "dynamic_param" with only int2
		// compatible type. Link with column param.

		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&commonmodels.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		err := columnParam.Init(vc, nil, commonmodels.ParamsValue("id2"))
		assert.False(t, vc.HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetCompatibleTypes("int2"),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		timestampParam := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "timestamp_column").
			Return(
				&commonmodels.Column{
					Idx:      1,
					Name:     "timestamp_column",
					TypeName: "timestamp",
					TypeOID:  12,
				},
				true,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", commonmodels.VirtualOID(10)).
			Return("int2", nil)
		tableDriver.On("GetCanonicalTypeName", "timestamp", commonmodels.VirtualOID(12)).
			Return("timestamp", nil)
		err = timestampParam.Init(
			vc,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			commonmodels.DynamicParamValue{
				Column: "timestamp_column",
			},
		)
		assert.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.True(t, slices.ContainsFunc(vc.GetWarnings(), func(w *commonmodels.ValidationWarning) bool {
			return strings.Contains(w.Msg, "linked parameter and dynamic parameter column name has different types")
		}))

		tableDriver.AssertExpectations(t)
	})

	t.Run("success", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&commonmodels.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		err := columnParam.Init(vc, nil, commonmodels.ParamsValue("id2"))
		assert.False(t, vc.HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetCompatibleTypes("int2"),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&commonmodels.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", commonmodels.VirtualOID(10)).
			Return("int2", nil).
			Twice()
		err = dynamicParameter.Init(
			vc,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			commonmodels.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, vc.HasWarnings())
		tableDriver.AssertExpectations(t)
	})
}

func TestDynamicParameter_Value(t *testing.T) {
	t.Run("common decoding", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&commonmodels.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		err := columnParam.Init(vc, nil, commonmodels.ParamsValue("id2"))
		assert.False(t, vc.HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetCompatibleTypes("int2"),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&commonmodels.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", commonmodels.VirtualOID(10)).
			Return("int2", nil).
			Twice()
		err = dynamicParameter.Init(
			vc,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			commonmodels.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, vc.HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &commonmodels.ColumnRawValue{
			Data:   []byte("123"),
			IsNull: false,
		}
		record.On("GetRawColumnValueByIdx", 1).
			Return(data, nil).
			Once()
		tableDriver.On("DecodeValueByColumnIdx", 1, data.Data).
			Return(int64(123), nil).
			Once()
		v, err := dynamicParameter.Value()
		require.NoError(t, err)
		assert.Equal(t, int64(123), v)
		tableDriver.AssertExpectations(t)
	})

	t.Run("with unmarshaler", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&commonmodels.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		err := columnParam.Init(vc, nil, commonmodels.ParamsValue("id2"))
		assert.False(t, vc.HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetCompatibleTypes("int2").
					SetUnmarshaler(func(driver commonininterfaces.DBMSDriver, typeName string, v commonmodels.ParamsValue) (any, error) {
						assert.Equal(t, "int2", typeName)
						assert.Equal(t, "1234", string(v))
						return int64(1234), nil
					}),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&commonmodels.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", commonmodels.VirtualOID(10)).
			Return("int2", nil).
			Twice()
		err = dynamicParameter.Init(
			vc,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			commonmodels.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, vc.HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &commonmodels.ColumnRawValue{
			Data:   []byte("1234"),
			IsNull: false,
		}
		record.On("GetRawColumnValueByIdx", 1).
			Return(data, nil).
			Once()
		v, err := dynamicParameter.Value()
		require.NoError(t, err)
		assert.Equal(t, int64(1234), v)
		tableDriver.AssertExpectations(t)
	})

	t.Run("with template", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		vc := validationcollector.NewCollector()

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&commonmodels.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		err := columnParam.Init(vc, nil, commonmodels.ParamsValue("id2"))
		assert.False(t, vc.HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetCompatibleTypes("int2"),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&commonmodels.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeOID:  10,
				},
				true,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", commonmodels.VirtualOID(10)).
			Return("int2", nil).
			Twice()
		err = dynamicParameter.Init(
			vc,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			commonmodels.DynamicParamValue{
				Column:   "supported_column",
				Template: `{{ .GetValue | add 1 | .EncodeValueByColumn "int2" }}`,
			},
		)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &commonmodels.ColumnRawValue{
			Data:   []byte("1234"),
			IsNull: false,
		}
		record.On("GetRawColumnValueByIdx", 1).
			Return(data, nil).
			Once()
		record.On("GetColumnValueByName", "supported_column").
			Return(&commonmodels.ColumnValue{Value: int64(1), IsNull: false}, nil)
		// The value is incremented so we have to return it.
		tableDriver.On("EncodeValueByColumnName", "int2", int64(2), []byte(nil)).
			Return([]byte("2"), nil)
		tableDriver.On("DecodeValueByColumnIdx", 1, []byte("2")).
			Return(int64(2), nil).
			Once()
		v, err := dynamicParameter.Value()
		require.NoError(t, err)
		assert.Equal(t, int64(2), v)
		tableDriver.AssertExpectations(t)
	})
}

//func TestDynamicParameter_Init_linked_column_parameter_supported_types(t *testing.T) {
//	driver, _ := GetDriverAndRecord(
//		map[string]*RawValue{
//			"id2":       NewRawValue([]byte("123"), false),
//			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
//			"date_date": NewRawValue([]byte("2024-01-12"), false),
//		},
//	)
//
//	columnDef := MustNewParameterDefinition("column", "some desc").
//		SetIsColumn(
//			NewColumnProperties().
//				SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
//		)
//
//	columnParam := NewStaticParameter(columnDef, driver)
//	warns, err := columnParam.Init(nil, ParamsValue("date_date"))
//	require.NoError(t, err)
//	require.Empty(t, warns)
//
//	timestampDef := MustNewParameterDefinition("ts_val", "some desc").
//		LinkParameter("column").
//		SetDynamicMode(
//			NewDynamicModeProperties().
//				SetCompatibleTypes("date", "timestamp", "timestamptz"),
//		)
//
//	timestampParam := NewDynamicParameter(timestampDef, driver)
//
//	warns, err = timestampParam.Init(
//		map[string]*StaticParameter{columnDef.ID: columnParam},
//		&DynamicParamValue{
//			Column: "date_tstz",
//		},
//	)
//	require.NoError(t, err)
//	require.Empty(t, warns)
//
//}

//
//func TestDynamicParameter_Value_simple(t *testing.T) {
//	driver, record := GetDriverAndRecord(
//		map[string]*RawValue{
//			"id2":       NewRawValue([]byte("123"), false),
//			"date_tstz": NewRawValue([]byte("2024-01-12 15:12:32.232749+00"), false),
//		},
//	)
//
//	timestampDef := MustNewParameterDefinition("ts_val", "some desc").
//		SetDynamicMode(
//			NewDynamicModeProperties().
//				SetCompatibleTypes("date", "timestamp", "timestamptz"),
//		)
//
//	timestampParam := NewDynamicParameter(timestampDef, driver)
//
//	warns, err := timestampParam.Init(
//		nil,
//		&DynamicParamValue{
//			Column: "date_tstz",
//		},
//	)
//	require.NoError(t, err)
//	require.Empty(t, warns)
//
//	timestampParam.SetRecord(record)
//
//	value, err := timestampParam.Value()
//	require.NoError(t, err)
//	require.NotEmpty(t, value)
//}
