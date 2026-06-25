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
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

type recorderMock struct {
	mock.Mock
}

func (r *recorderMock) SetRow(rawRecord [][]byte) error {
	args := r.Called(rawRecord)
	return args.Error(0)
}

func (r *recorderMock) GetRow() [][]byte {
	args := r.Called()
	return args.Get(0).([][]byte)
}

func (r *recorderMock) ScanColumnValueByIdx(idx int, v any) (bool, error) {
	args := r.Called(idx, v)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) ScanColumnValueByName(name string, v any) (bool, error) {
	args := r.Called(name, v)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) IsNullByColumnName(columName string) (bool, error) {
	args := r.Called(columName)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) IsNullByColumnIdx(columIdx int) (bool, error) {
	args := r.Called(columIdx)
	return args.Bool(0), args.Error(1)
}

func (r *recorderMock) GetRawColumnValueByIdx(columnIdx int) (*core.ColumnRawValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.ColumnRawValue), nil
}

func (r *recorderMock) GetColumnValueByIdx(columnIdx int) (*core.ColumnValue, error) {
	args := r.Called(columnIdx)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.ColumnValue), nil
}

func (r *recorderMock) GetColumnValueByName(columnName string) (*core.ColumnValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.ColumnValue), nil
}

func (r *recorderMock) GetRawColumnValueByName(columnName string) (*core.ColumnRawValue, error) {
	args := r.Called(columnName)
	if args.Get(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.ColumnRawValue), nil
}

func (r *recorderMock) SetColumnValueByIdx(columnIdx int, v any) error {
	args := r.Called(columnIdx, v)
	return args.Error(0)
}

func (r *recorderMock) SetRawColumnValueByIdx(columnIdx int, value *core.ColumnRawValue) error {
	args := r.Called(columnIdx, value)
	return args.Error(0)
}

func (r *recorderMock) SetColumnValueByName(columnName string, v any) error {
	args := r.Called(columnName, v)
	return args.Error(0)
}

func (r *recorderMock) SetRawColumnValueByName(columnName string, value *core.ColumnRawValue) error {
	args := r.Called(columnName, value)
	return args.Error(0)
}

func (r *recorderMock) GetColumnByName(columnName string) (*core.Column, error) {
	args := r.Called(columnName)
	if args.Error(1) != nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.Column), nil
}

func (r *recorderMock) TableDriver() core.TableDriver {
	args := r.Called()
	return args.Get(0).(core.TableDriver)
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

func (t *tableDriverMock) EncodeValueByTypeID(oid core.TypeID, src any, buf []byte) ([]byte, error) {
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

func (t *tableDriverMock) DecodeValueByTypeID(oid core.TypeID, src []byte) (any, error) {
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

func (t *tableDriverMock) ScanValueByTypeID(oid core.TypeID, src []byte, dest any) error {
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

func (t *tableDriverMock) TypeExistsByID(oid core.TypeID) bool {
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

func (t *tableDriverMock) GetTypeID(name string) (core.TypeID, error) {
	args := t.Called(name)
	if args.Get(0) == nil {
		return core.TypeID(0), args.Error(1)
	}
	oid, ok := args.Get(0).(core.TypeID)
	if !ok {
		panic(fmt.Sprintf("expected commonmodels.TypeID, got %T", args.Get(0)))
	}
	return oid, args.Error(1)
}

func (t *tableDriverMock) GetCanonicalTypeName(typeName string, typeOid core.TypeID) (string, error) {
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
	return args.Get(0), args.Error(1)
}

func (t *tableDriverMock) DecodeValueByColumnName(name string, src []byte) (any, error) {
	args := t.Called(name, src)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0), args.Error(1)
}

func (t *tableDriverMock) GetColumnByName(name string) (*core.Column, error) {
	args := t.Called(name)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*core.Column), args.Error(1)
}

func (t *tableDriverMock) Table() *core.Table {
	args := t.Called()
	if args.Get(0) == nil {
		return nil
	}
	table, ok := args.Get(0).(*core.Table)
	if !ok {
		panic(fmt.Sprintf("expected *commonmodels.Table, got %T", args.Get(0)))
	}
	return table
}

func (t *tableDriverMock) GetCanonicalTypeClassName(typeName string, typeOid core.TypeID) (core.TypeClass, error) {
	args := t.Called(typeName, typeOid)
	return args.Get(0).(core.TypeClass), args.Error(1)
}

func TestDynamicParameter_Init(t *testing.T) {
	t.Run("error column param cannot be dynamic", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("text"),
			)
		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		parameter := NewDynamicParameter(columnDef, tableDriver)
		err := parameter.Init(ctx, nil, core.DynamicParamValue{
			Column: "data",
		})
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		require.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, slices.ContainsFunc(validationcollector.FromContext(ctx).GetWarnings(),
			func(w *core.ValidationWarning) bool {
				return strings.Contains(w.Msg, "parameter does not support dynamic mode")
			}))
	})

	t.Run("linked column parameter and unsupported type", func(t *testing.T) {
		// First create a column parameter with supported INT types.
		// Then create a dynamic parameter "dynamic_param" with only int2
		// compatible type. Link with column param.

		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver, false)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&core.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		err := columnParam.Init(ctx, nil, core.ParamsValue("id2"))
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetColumnProperties(core.NewColumnProperties().
						SetAllowedColumnTypes("int2")),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		timestampParam := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "timestamp_column").
			Return(
				&core.Column{
					Idx:      1,
					Name:     "timestamp_column",
					TypeName: "timestamp",
					TypeID:   12,
				},
				nil,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", core.TypeID(10)).
			Return("int2", nil)
		tableDriver.On("GetCanonicalTypeClassName", "int2", core.TypeID(10)).
			Return(core.TypeClassInt, nil)
		tableDriver.On("GetCanonicalTypeName", "timestamp", core.TypeID(12)).
			Return("timestamp", nil)
		tableDriver.On("GetCanonicalTypeClassName", "timestamp", core.TypeID(12)).
			Return(core.TypeClassDateTime, nil)
		err = timestampParam.Init(
			ctx,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			core.DynamicParamValue{
				Column: "timestamp_column",
			},
		)
		assert.ErrorIs(t, err, core.ErrFatalValidationError)
		assert.True(t, validationcollector.FromContext(ctx).IsFatal())
		assert.True(t, slices.ContainsFunc(validationcollector.FromContext(ctx).GetWarnings(),
			func(w *core.ValidationWarning) bool {
				return strings.Contains(w.Msg, "linked parameter and dynamic parameter column name has different types")
			}))

		tableDriver.AssertExpectations(t)
	})

	t.Run("success", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver, false)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&core.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		err := columnParam.Init(ctx, nil, core.ParamsValue("id2"))
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetColumnProperties(core.NewColumnProperties().
						SetAllowedColumnTypes("int2")),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&core.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", core.TypeID(10)).
			Return("int2", nil).
			Twice()
		tableDriver.On("GetCanonicalTypeClassName", "int2", core.TypeID(10)).
			Return(core.TypeClassInt, nil).
			Twice()
		err = dynamicParameter.Init(
			ctx,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			core.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		tableDriver.AssertExpectations(t)
	})
}

func TestDynamicParameter_Value(t *testing.T) {
	t.Run("common decoding", func(t *testing.T) {
		columnDef := MustNewParameterDefinition("column", "some desc").
			SetIsColumn(
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver, false)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&core.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		err := columnParam.Init(ctx, nil, core.ParamsValue("id2"))
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetColumnProperties(core.NewColumnProperties().
						SetAllowedColumnTypes("int2")),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&core.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", core.TypeID(10)).
			Return("int2", nil).
			Twice()
		tableDriver.On("GetCanonicalTypeClassName", "int2", core.TypeID(10)).
			Return(core.TypeClassInt, nil).
			Twice()
		err = dynamicParameter.Init(
			ctx,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			core.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &core.ColumnRawValue{
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
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver, false)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&core.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		err := columnParam.Init(ctx, nil, core.ParamsValue("id2"))
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetColumnProperties(core.NewColumnProperties().
						SetAllowedColumnTypes("int2")).
					SetUnmarshaler(func(driver core.DBMSDriver, typeName string, v core.ParamsValue) (any, error) {
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
				&core.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", core.TypeID(10)).
			Return("int2", nil).
			Twice()
		tableDriver.On("GetCanonicalTypeClassName", "int2", core.TypeID(10)).
			Return(core.TypeClassInt, nil).
			Twice()
		err = dynamicParameter.Init(
			ctx,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			core.DynamicParamValue{
				Column: "supported_column",
			},
		)
		assert.NoError(t, err)
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &core.ColumnRawValue{
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
				core.NewColumnProperties().
					SetAllowedColumnTypes("int2", "int4", "int8"),
			)

		ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())

		tableDriver := newTableDriverMock()
		columnParam := NewStaticParameter(columnDef, tableDriver, false)
		tableDriver.On("GetColumnByName", "id2").
			Return(
				&core.Column{
					Idx:      0,
					Name:     "id2",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		err := columnParam.Init(ctx, nil, core.ParamsValue("id2"))
		assert.False(t, validationcollector.FromContext(ctx).HasWarnings())
		require.NoError(t, err)

		dynamicParamDef := MustNewParameterDefinition("dynamic_param", "some desc").
			LinkParameter("column").
			SetDynamicMode(
				NewDynamicModeProperties().
					SetColumnProperties(core.NewColumnProperties().
						SetAllowedColumnTypes("int2")),
			)

		// Initialize dynamic parameter with a timestamp column. So it is not supported by
		// dynamic param.
		dynamicParameter := NewDynamicParameter(dynamicParamDef, tableDriver)
		tableDriver.On("GetColumnByName", "supported_column").
			Return(
				&core.Column{
					Idx:      1,
					Name:     "supported_column",
					TypeName: "int2",
					TypeID:   10,
				},
				nil,
			)
		tableDriver.On("GetCanonicalTypeName", "int2", core.TypeID(10)).
			Return("int2", nil).
			Twice()
		tableDriver.On("GetCanonicalTypeClassName", "int2", core.TypeID(10)).
			Return(core.TypeClassInt, nil).
			Twice()
		err = dynamicParameter.Init(
			ctx,
			map[string]*StaticParameter{columnDef.Name: columnParam},
			core.DynamicParamValue{
				Column:   "supported_column",
				Template: `{{ .GetValue | add 1 | .EncodeValueByColumn "int2" }}`,
			},
		)
		require.NoError(t, err)
		require.False(t, validationcollector.FromContext(ctx).HasWarnings())

		record := newRecorderMock()
		record.On("TableDriver").
			Return(tableDriver)
		dynamicParameter.SetRecord(record)
		data := &core.ColumnRawValue{
			Data:   []byte("1234"),
			IsNull: false,
		}
		record.On("GetRawColumnValueByIdx", 1).
			Return(data, nil).
			Once()
		record.On("GetColumnValueByName", "supported_column").
			Return(&core.ColumnValue{Value: int64(1), IsNull: false}, nil)
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
//		SetColumnProperties(
//			config.NewColumnProperties().
//				SetAllowedColumnTypes("date", "timestamp", "timestamptz"),
//		)
//
//	columnParam := NewStaticParameter(columnDef, driver)
//	warns, err := columnParam.PrepareTxPool(nil, ParamsValue("date_date"))
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
//	warns, err = timestampParam.PrepareTxPool(
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
//	warns, err := timestampParam.PrepareTxPool(
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
