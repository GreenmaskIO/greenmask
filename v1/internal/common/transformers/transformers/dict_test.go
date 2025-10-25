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
	t.Run("success no validate", func(t *testing.T) {
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
			"validate":         validateParameter,
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
		assert.Equal(t, replaceTransformer.validate, false)
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

	t.Run("success validate", func(t *testing.T) {
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
				require.NoError(t, utils.ScanPointer(true, dest))
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
		tableDriver.On("DecodeValueByColumnIdx", column.Idx, []byte("1")).Return([]byte("1"), nil)
		tableDriver.On("DecodeValueByColumnIdx", column.Idx, []byte("2")).Return([]byte("2"), nil)

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
			"validate":         validateParameter,
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
		assert.Equal(t, replaceTransformer.validate, true)
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

func TestDictTransformer_Transform(t *testing.T) {
	t.Run("fail: null - no default - fail not matched", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewDictTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "id",
				TypeName: "int",
				TypeOID:  23,
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
			withParameter("values", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						values := map[string]string{
							"1": "2",
						}
						require.NoError(t, utils.ScanPointer(values, dest))
					}).Return(nil)
			}),
			withParameter("validate", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("default", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("IsEmpty").
					Return(true, nil)
			}),
			withParameter("fail_not_matched", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("id").Idx).
					Return(commonmodels.NewColumnRawValue(nil, true), nil)
			}),
		)

		err := env.transform()
		require.ErrorIs(t, err, ErrDictTransformerFailNotMatched)
		env.assertExpectations(t)
	})

	t.Run("success: null - default - fail not matched", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewDictTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "id",
				TypeName: "int",
				TypeOID:  23,
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
			withParameter("values", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						values := map[string]string{
							"1": "2",
						}
						require.NoError(t, utils.ScanPointer(values, dest))
					}).Return(nil)
			}),
			withParameter("validate", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("default", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("IsEmpty").
					Return(false, nil)
				param.On("RawValue").Return(commonmodels.ParamsValue("100"), nil)
			}),
			withParameter("fail_not_matched", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("id").Idx).
					Return(commonmodels.NewColumnRawValue(nil, true), nil)
				recorder.On("SetRawColumnValueByIdx", env.getColumn("id").Idx,
					mock.MatchedBy(func(value *commonmodels.ColumnRawValue) bool {
						return value != nil && !value.IsNull && string(value.Data) == "100"
					})).
					Return(nil).
					Once()
			}),
		)

		err := env.transform()
		require.NoError(t, err, ErrDictTransformerFailNotMatched)
		env.assertExpectations(t)
	})

	t.Run("success: match null", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewDictTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "id",
				TypeName: "int",
				TypeOID:  23,
			}),
			withParameter(ParameterNameColumn, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(env.columns["id"].Name, dest))
					},
					).Return(nil)
			}),
			func(env *transformerTestEnv) {
				// Setup get column call for driver during initialization.
				env.tableDriver.On("GetColumnByName", "id").
					Return(env.getColumnPtr("id"), nil)
			},
			withParameter("values", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						values := map[string]string{
							defaultNullSeq: "2",
						}
						require.NoError(t, utils.ScanPointer(values, dest))
					}).Return(nil)
			}),
			withParameter("validate", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("default", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("IsEmpty").
					Return(true, nil)
			}),
			withParameter("fail_not_matched", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("id").Idx).
					Return(commonmodels.NewColumnRawValue(nil, true), nil)
				recorder.On("SetRawColumnValueByIdx", env.getColumn("id").Idx,
					mock.MatchedBy(func(value *commonmodels.ColumnRawValue) bool {
						return value != nil && !value.IsNull && string(value.Data) == "2"
					})).
					Return(nil).
					Once()
			}),
		)

		err := env.transform()
		require.NoError(t, err)
		env.assertExpectations(t)
	})

	t.Run("success: match not null", func(t *testing.T) {
		env := newTransformerTestEnv(t, NewDictTransformer,
			withColumns(commonmodels.Column{
				Idx:      0,
				Name:     "id",
				TypeName: "int",
				TypeOID:  23,
			}),
			withParameter(ParameterNameColumn, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(env.columns["id"].Name, dest))
					},
					).Return(nil)
			}),
			func(env *transformerTestEnv) {
				// Setup get column call for driver during initialization.
				env.tableDriver.On("GetColumnByName", "id").
					Return(env.getColumnPtr("id"), nil)
			},
			withParameter("values", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						values := map[string]string{
							"3": "2",
						}
						require.NoError(t, utils.ScanPointer(values, dest))
					}).Return(nil)
			}),
			withParameter("validate", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(false, dest))
					}).Return(nil)
			}),
			withParameter("default", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("IsEmpty").
					Return(true, nil)
			}),
			withParameter("fail_not_matched", func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(t, utils.ScanPointer(true, dest))
					}).Return(nil)
			}),
			withRecorder(func(recorder *mocks.RecorderMock, env *transformerTestEnv) {
				recorder.On("GetRawColumnValueByIdx", env.getColumn("id").Idx).
					Return(commonmodels.NewColumnRawValue([]byte("3"), false), nil)
				recorder.On("SetRawColumnValueByIdx", env.getColumn("id").Idx,
					mock.MatchedBy(func(value *commonmodels.ColumnRawValue) bool {
						return value != nil && !value.IsNull && string(value.Data) == "2"
					})).
					Return(nil).
					Once()
			}),
		)

		err := env.transform()
		require.NoError(t, err)
		env.assertExpectations(t)
	})
}
