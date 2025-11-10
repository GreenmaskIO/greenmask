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

package pipeline

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	dumpcontext "github.com/greenmaskio/greenmask/v1/internal/common/dump/context"
	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	transformerstesting "github.com/greenmaskio/greenmask/v1/internal/common/transformers/testing"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

type whenMock struct {
	mock.Mock
}

func (w *whenMock) Evaluate(r commonininterfaces.Recorder) (bool, error) {
	args := w.Called(r)
	return args.Bool(0), args.Error(1)
}

type transformerMock struct {
	mock.Mock
}

func (t *transformerMock) Describe() string {
	args := t.Called()
	return args.String(0)
}

func (t *transformerMock) Init(ctx context.Context) error {
	args := t.Called(ctx)
	return args.Error(0)
}

func (t *transformerMock) Done(ctx context.Context) error {
	args := t.Called(ctx)
	return args.Error(0)
}

func (t *transformerMock) Transform(ctx context.Context, r commonininterfaces.Recorder) error {
	args := t.Called(ctx, r)
	return args.Error(0)
}

func (t *transformerMock) GetAffectedColumns() map[int]string {
	args := t.Called()
	return args.Get(0).(map[int]string)
}

func TestTransformerBase_Init(t *testing.T) {
	t.Run("init error of the second tran", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:       0,
				Name:      "first_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       1,
				Name:      "last_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       2,
				Name:      "middle_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
		}
		table := commonmodels.Table{
			Schema:  "public",
			Name:    "users",
			Columns: columns,
		}

		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)

		tran1 := &transformerMock{}
		tran1.On("Init", mock.Anything).
			Return(nil)
		tran1.On("Done", mock.Anything).
			Return(nil)

		tranCtx1 := &dumpcontext.TransformerContext{
			Transformer: tran1,
		}

		tran2 := &transformerMock{}
		tran2.On("Init", mock.Anything).
			Return(assert.AnError)
		tran2.On("Describe").
			Return("TestTran2")

		tranCtx2 := &dumpcontext.TransformerContext{
			Transformer: tran2,
		}

		tableCond := &whenMock{}

		tableContext := &dumpcontext.TableContext{
			Table: &table,
			TransformerContext: []*dumpcontext.TransformerContext{
				tranCtx1,
				tranCtx2,
			},
			Condition:   tableCond,
			TableDriver: nil,
		}

		tp := NewTransformationPipeline(tableContext)
		err := tp.Init(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "initialize transformer 'TestTran2'[1]")

		tran1.AssertExpectations(t)
		tran2.AssertExpectations(t)
		tableCond.AssertExpectations(t)
	})
}

func TestTransformerBase_Transform(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:       0,
				Name:      "first_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       1,
				Name:      "last_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       2,
				Name:      "middle_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
		}
		table := commonmodels.Table{
			Schema:  "public",
			Name:    "users",
			Columns: columns,
		}

		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)

		tran1 := &transformerMock{}
		tran1.On("Init", mock.Anything).
			Return(nil)
		tran1.On("Transform", mock.Anything, mock.Anything).
			Return(nil)
		tran1.On("Done", mock.Anything).
			Return(nil)

		tranCond1 := &whenMock{}
		tranCond1.On("Evaluate", mock.Anything).
			Return(true, nil)
		tranCtx1 := &dumpcontext.TransformerContext{
			Transformer: tran1,
			Condition:   tranCond1,
		}

		tran2 := &transformerMock{}
		tran2.On("Init", mock.Anything).
			Return(nil)
		tran2.On("Transform", mock.Anything, mock.Anything).
			Return(nil)
		tran2.On("Done", mock.Anything).
			Return(nil)

		tranCond2 := &whenMock{}
		tranCond2.On("Evaluate", mock.Anything).
			Return(true, nil)
		tranCtx2 := &dumpcontext.TransformerContext{
			Transformer: tran2,
			Condition:   tranCond2,
		}

		tableCond := &whenMock{}
		tableCond.On("Evaluate", mock.Anything).
			Return(true, nil)

		tableContext := &dumpcontext.TableContext{
			Table: &table,
			TransformerContext: []*dumpcontext.TransformerContext{
				tranCtx1,
				tranCtx2,
			},
			Condition:   tableCond,
			TableDriver: nil,
		}

		tp := NewTransformationPipeline(tableContext)
		err := tp.Init(context.Background())
		require.NoError(t, err)

		err = tp.Transform(context.Background(), env.Recorder)
		require.NoError(t, err)

		err = tp.Done(context.Background())
		require.NoError(t, err)

		tran1.AssertExpectations(t)
		tran2.AssertExpectations(t)
		tableCond.AssertExpectations(t)
		tranCond1.AssertExpectations(t)
		tranCond2.AssertExpectations(t)
	})

	t.Run("without conds", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:       0,
				Name:      "first_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       1,
				Name:      "last_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
			{
				Idx:       2,
				Name:      "middle_name",
				TypeName:  mysqldbmsdriver.TypeText,
				TypeOID:   mysqldbmsdriver.VirtualOidText,
				TypeClass: mysqldbmsdriver.TypeText,
				Length:    0,
			},
		}
		table := commonmodels.Table{
			Schema:  "public",
			Name:    "users",
			Columns: columns,
		}

		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)

		tran1 := &transformerMock{}
		tran1.On("Init", mock.Anything).
			Return(nil)
		tran1.On("Transform", mock.Anything, mock.Anything).
			Return(nil)
		tran1.On("Done", mock.Anything).
			Return(nil)

		tranCtx1 := &dumpcontext.TransformerContext{
			Transformer: tran1,
		}

		tran2 := &transformerMock{}
		tran2.On("Init", mock.Anything).
			Return(nil)
		tran2.On("Transform", mock.Anything, mock.Anything).
			Return(nil)
		tran2.On("Done", mock.Anything).
			Return(nil)

		tranCtx2 := &dumpcontext.TransformerContext{
			Transformer: tran2,
		}

		tableCond := &whenMock{}
		tableCond.On("Evaluate", mock.Anything).
			Return(true, nil)

		tableContext := &dumpcontext.TableContext{
			Table: &table,
			TransformerContext: []*dumpcontext.TransformerContext{
				tranCtx1,
				tranCtx2,
			},
			Condition:   tableCond,
			TableDriver: nil,
		}

		tp := NewTransformationPipeline(tableContext)
		err := tp.Init(context.Background())
		require.NoError(t, err)

		err = tp.Transform(context.Background(), env.Recorder)
		require.NoError(t, err)

		err = tp.Done(context.Background())
		require.NoError(t, err)

		tran1.AssertExpectations(t)
		tran2.AssertExpectations(t)
		tableCond.AssertExpectations(t)
	})
}
