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

func TestNewReplaceTransformer(t *testing.T) {
	t.Run("success static no validate no keep null", func(t *testing.T) {
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
		tableDriver.On("GetColumnByName", column.Name).Return(&column, nil)

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
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		tr, err := NewReplaceTransformer(ctx, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*ReplaceTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.needValidate, false)
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

	t.Run("success static and validate and valid and not keep null", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
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
		tableDriver.On("GetColumnByName", column.Name).Return(&column, nil)

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
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		tr, err := NewReplaceTransformer(ctx, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*ReplaceTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.needValidate, true)
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

	t.Run("failure static and validate and invalid and not keep null", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
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
		tableDriver.On("GetColumnByName", column.Name).Return(&column, nil)

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
			Return("validate")

		// Keep null parameter calls
		keepNullParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		_, err := NewReplaceTransformer(ctx, tableDriver, parameters)
		require.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		assert.True(t, vc.IsFatal())
		assert.Contains(t, vc.GetWarnings()[0].Msg, "error validating parameter value")

		tableDriver.AssertExpectations(t)
		columnParameter.AssertExpectations(t)
		validateParameter.AssertExpectations(t)
		valueParameter.AssertExpectations(t)
		keepNullParameter.AssertExpectations(t)
	})

	t.Run("dynamic and validate", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
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

		// column parameter calls
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
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)

		// Value parameter calls
		valueParameter.On("IsDynamic").
			Return(true)

		// Keep null parameter calls
		keepNullParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(false, dest))
			}).Return(nil)

		parameters := map[string]commonparameters.Parameterizer{
			"column":    columnParameter,
			"value":     valueParameter,
			"validate":  validateParameter,
			"keep_null": keepNullParameter,
		}

		tr, err := NewReplaceTransformer(ctx, tableDriver, parameters)
		require.NoError(t, err)
		assert.NotNil(t, tr)
		assert.False(t, vc.HasWarnings())
		replaceTransformer := tr.(*ReplaceTransformer)
		assert.Equal(t, replaceTransformer.columnName, column.Name)
		assert.Equal(t, replaceTransformer.columnIdx, column.Idx)
		assert.Equal(t, replaceTransformer.affectedColumns, map[int]string{1: "id"})
		assert.Equal(t, replaceTransformer.needValidate, true)
		assert.Equal(t, replaceTransformer.columnOIDToValidate, column.TypeOID)

		expectedTransformationFunc := reflect.ValueOf(replaceTransformer.transformDynamic).Pointer()
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
}

type replaceTestSetup struct {
	tableDriver   *mocks.TableDriverMock
	columnParam   *mocks.ParametrizerMock
	validateParam *mocks.ParametrizerMock
	valueParam    *mocks.ParametrizerMock
	keepNullParam *mocks.ParametrizerMock
	collector     *validationcollector.Collector
	column        commonmodels.Column
	transformer   *ReplaceTransformer
}

func (m *replaceTestSetup) assertExpectations(t *testing.T) {
	m.tableDriver.AssertExpectations(t)
	m.columnParam.AssertExpectations(t)
	m.validateParam.AssertExpectations(t)
	m.valueParam.AssertExpectations(t)
	m.keepNullParam.AssertExpectations(t)
}

func newTestReplaceTransformer(t *testing.T, opt ...func(*replaceTestSetup)) *replaceTestSetup {
	t.Helper()

	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	column := commonmodels.Column{
		Idx:      1,
		Name:     "id",
		TypeName: "int",
		TypeOID:  2,
	}

	setup := &replaceTestSetup{
		tableDriver:   mocks.NewTableDriverMock(),
		columnParam:   mocks.NewParametrizerMock(),
		validateParam: mocks.NewParametrizerMock(),
		valueParam:    mocks.NewParametrizerMock(),
		keepNullParam: mocks.NewParametrizerMock(),
		collector:     vc,
		column:        column,
	}

	// Allow test-specific overrides
	for _, o := range opt {
		o(setup)
	}

	parameters := map[string]commonparameters.Parameterizer{
		"column":    setup.columnParam,
		"value":     setup.valueParam,
		"validate":  setup.validateParam,
		"keep_null": setup.keepNullParam,
	}

	transformer, err := NewReplaceTransformer(ctx, setup.tableDriver, parameters)
	require.NoError(t, err)
	setup.transformer = transformer.(*ReplaceTransformer)

	return setup
}

func TestReplaceTransformer_Transform(t *testing.T) {
	t.Run("static non null no validate", func(t *testing.T) {
		env := newTransformerTestEnv(t,
			NewReplaceTransformer,
			withColumns(commonmodels.Column{
				Idx:      1,
				Name:     "id",
				TypeName: "int",
				TypeOID:  2,
			}),
			withParameter(ParameterNameColumn, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(env.columns["id"].Name, dest))
					}).Return(nil)
			}),
			func(env *transformerTestEnv) {
				// Setup get column call for driver during initialization.
				env.tableDriver.On("GetColumnByName", "id").
					Return(env.getColumnPtr("id"), nil)
			},
			withParameter("validate", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("value", func(param *mocks.ParametrizerMock, e *transformerTestEnv) {
				param.On("IsDynamic").
					Return(false)
				param.On("RawValue").
					Return(commonmodels.ParamsValue("123"), nil)
			}),
			withParameter(ParameterNameKeepNull, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("IsNullByColumnIdx", env.getColumn("id").Idx).
					Return(false, nil)
				recorder.On("SetRawColumnValueByIdx",
					env.getColumn("id").Idx, commonmodels.NewColumnRawValue([]byte("123"), false),
				).Return(nil)
			}),
		)

		err := env.transform()
		require.NoError(t, err)
		env.assertExpectations(t)
	})

	t.Run("static null no validate replace null", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).
				Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(false, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(false)
			setup.valueParam.On("RawValue").
				Return(commonmodels.ParamsValue("123"), nil)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(false, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("SetRawColumnValueByIdx",
			setup.column.Idx, commonmodels.NewColumnRawValue([]byte("123"), false),
		).Return(nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})

	t.Run("static null no validate keep null", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).
				Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(false, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(false)
			setup.valueParam.On("RawValue").
				Return(commonmodels.ParamsValue("123"), nil)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("IsNullByColumnIdx", setup.column.Idx).
			Return(true, nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})

	t.Run("dynamic non null no validate", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(false, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(true)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("IsNullByColumnIdx", setup.column.Idx).
			Return(false, nil)
		recorder.On("TableDriver").Return(setup.tableDriver)
		setup.valueParam.On("IsEmpty").
			Return(false, nil)
		setup.valueParam.On("RawValue").
			Return(commonmodels.ParamsValue("123"), nil)
		recorder.On("SetRawColumnValueByIdx",
			setup.column.Idx, commonmodels.NewColumnRawValue([]byte("123"), false),
		).Return(nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})

	t.Run("dynamic non null validate", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(true)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("IsNullByColumnIdx", setup.column.Idx).
			Return(false, nil)
		recorder.On("TableDriver").Return(setup.tableDriver)
		setup.valueParam.On("IsEmpty").
			Return(false, nil)
		setup.valueParam.On("RawValue").
			Return(commonmodels.ParamsValue("123"), nil)
		setup.tableDriver.On("DecodeValueByTypeOid", setup.column.TypeOID, []byte("123")).
			Return(int64(123), nil)
		recorder.On("SetRawColumnValueByIdx",
			setup.column.Idx, commonmodels.NewColumnRawValue([]byte("123"), false),
		).Return(nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})

	t.Run("dynamic null no validate keep null", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(true)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("IsNullByColumnIdx", setup.column.Idx).
			Return(true, nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})

	t.Run("dynamic null validate replace null and param value is empty", func(t *testing.T) {
		setup := newTestReplaceTransformer(t, func(setup *replaceTestSetup) {
			setup.columnParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(setup.column.Name, dest))
				}).Return(nil)
			setup.tableDriver.On("GetColumnByName", setup.column.Name).Return(&setup.column, nil)

			// Validate parameter calls
			setup.validateParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(true, dest))
				}).Return(nil)

			// Value parameter calls
			setup.valueParam.On("IsDynamic").
				Return(true)

			// Keep null parameter calls
			setup.keepNullParam.On("Scan", mock.Anything).
				Run(func(args mock.Arguments) {
					dest := args.Get(0)
					require.NoError(t, utils.ScanPointer(false, dest))
				}).Return(nil)
		})

		recorder := mocks.NewRecorderMock()
		recorder.On("TableDriver").Return(setup.tableDriver)
		setup.valueParam.On("IsEmpty").
			Return(true, nil)
		recorder.On("SetRawColumnValueByIdx",
			setup.column.Idx, commonmodels.NewColumnRawValue(nil, true),
		).Return(nil)
		err := setup.transformer.Transform(context.Background(), recorder)
		require.NoError(t, err)
		setup.assertExpectations(t)
		recorder.AssertExpectations(t)
	})
}

func TestReplaceTransformer_GetAffectedColumns(t *testing.T) {
	tr := &ReplaceTransformer{
		affectedColumns: map[int]string{1: "id"},
	}
	require.Equal(t, tr.GetAffectedColumns(), map[int]string{1: "id"})
}

func TestReplaceTransformer_Init(t *testing.T) {
	tr := &ReplaceTransformer{}
	require.NoError(t, tr.Init(context.Background()))
}

func TestReplaceTransformer_Done(t *testing.T) {
	tr := &ReplaceTransformer{}
	require.NoError(t, tr.Done(context.Background()))
}
