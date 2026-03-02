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

package context

import (
	"context"
	"testing"

	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	mocks2 "github.com/greenmaskio/greenmask/pkg/common/mocks"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	parameters2 "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	utils2 "github.com/greenmaskio/greenmask/pkg/common/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer_Produce(t *testing.T) {
	tables := []models.Table{
		{
			Schema: "public",
			Name:   "test",
			Columns: []models.Column{
				{
					Idx:      0,
					Name:     "id",
					TypeName: "integer",
					TypeOID:  0,
				},
				{
					Idx:      1,
					Name:     "title",
					TypeName: "text",
					TypeOID:  1,
				},
				{
					Idx:      2,
					Name:     "created_at",
					TypeName: "timestamp",
					TypeOID:  2,
				},
				{
					Idx:      3,
					Name:     "json_data",
					TypeName: "jsonb",
					TypeOID:  3,
				},
				{
					Idx:      4,
					Name:     "float_data",
					TypeName: "float8",
					TypeOID:  4,
				},
			},
		},
	}

	t.Run("empty dump queries, table, empty transformers", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		//_, newFunc := mocks.NewTransformerMock()

		tr := registry.NewTransformerRegistry()
		//tr.MustRegister(
		//	transformerutils.NewTransformerDefinition(
		//		transformerutils.NewTransformerProperties("Test", "test desc"),
		//		newFunc,
		//		parameters.MustNewParameterDefinition("Test", "test desc").
		//			SetColumnProperties(
		//				models.NewColumnProperties().
		//					SetAllowedColumnTypes("int"),
		//			),
		//	),
		//)

		p := New(tables, dumpQueries, nil, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 0)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Empty(t, tableRuntimes[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
	})

	t.Run("dump query", func(t *testing.T) {
		dumpQueries := []string{
			"SELECT * FROM public.test;",
		}
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		tr := registry.NewTransformerRegistry()

		p := New(tables, dumpQueries, nil, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 0)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Equal(t, tableRuntimes[0].Query, dumpQueries[0])
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
	})

	t.Run("tableConfigs with table condition", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				When:   `id == 1`,
			},
		}

		tr := registry.NewTransformerRegistry()

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 0)
		require.NotNil(t, tableRuntimes[0].Condition)
		require.Empty(t, tableRuntimes[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
	})

	t.Run("tableConfigs with table condition", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
			},
		}

		tr := registry.NewTransformerRegistry()

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 0)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Empty(t, tableRuntimes[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
	})

	t.Run("config query", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				Query:  "SELECT * FROM public.test WHERE id == 1",
			},
		}

		tr := registry.NewTransformerRegistry()

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 0)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Equal(t, tableRuntimes[0].Query, tableConfigs[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
	})

	t.Run("transformer is successfully initialized with cond", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		tableDriverMock.On("GetColumnByName", "id").
			Return(
				&models.Column{
					Idx:      1,
					Name:     "id",
					TypeName: "int",
					TypeOID:  12,
				},
				nil,
			)
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		transformerMock, newFunc := mocks2.NewTransformerMock(func(
			ctx context.Context,
			tableDriver commonininterfaces.TableDriver,
			parameters map[string]parameters2.Parameterizer,
		) error {
			assert.Equal(t, tableDriverMock, tableDriver)
			v := utils.Must(parameters["column"].Value())
			require.Equal(t, v, "id")
			return nil
		})

		tr := registry.NewTransformerRegistry()
		tr.MustRegister(
			utils2.NewTransformerDefinition(
				utils2.NewTransformerProperties("TestTransformer", "test desc"),
				newFunc,
				parameters2.MustNewParameterDefinition("column", "test desc").
					SetIsColumn(
						models.NewColumnProperties().
							SetAllowedColumnTypes("int"),
					),
			),
		)

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				Transformers: []models.TransformerConfig{
					{
						Name: "TestTransformer",
						StaticParams: map[string]models.ParamsValue{
							"column": models.ParamsValue("id"),
						},
						When: `id == 1`,
					},
				},
			},
		}

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 1)
		require.Equal(t, tableRuntimes[0].TransformerContext[0].Transformer, transformerMock)
		require.NotNil(t, tableRuntimes[0].TransformerContext[0].Condition)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Equal(t, tableRuntimes[0].Query, tableConfigs[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
		transformerMock.AssertExpectations(t)
	})

	t.Run("transformer is successfully initialized without cond", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		tableDriverMock.On("GetColumnByName", "id").
			Return(
				&models.Column{
					Idx:      1,
					Name:     "id",
					TypeName: "int",
					TypeOID:  12,
				},
				nil,
			)
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		transformerMock, newFunc := mocks2.NewTransformerMock(func(
			ctx context.Context,
			tableDriver commonininterfaces.TableDriver,
			parameters map[string]parameters2.Parameterizer,
		) error {
			assert.Equal(t, tableDriverMock, tableDriver)
			v := utils.Must(parameters["column"].Value())
			require.Equal(t, v, "id")
			return nil
		})

		tr := registry.NewTransformerRegistry()
		tr.MustRegister(
			utils2.NewTransformerDefinition(
				utils2.NewTransformerProperties("TestTransformer", "test desc"),
				newFunc,
				parameters2.MustNewParameterDefinition("column", "test desc").
					SetIsColumn(
						models.NewColumnProperties().
							SetAllowedColumnTypes("int"),
					),
			),
		)

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				Transformers: []models.TransformerConfig{
					{
						Name: "TestTransformer",
						StaticParams: map[string]models.ParamsValue{
							"column": models.ParamsValue("id"),
						},
					},
				},
			},
		}

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		tableRuntimes, err := p.Build(ctx)
		require.NoError(t, err)
		require.False(t, vc.HasWarnings())
		require.Len(t, tableRuntimes, 1)
		require.Equal(t, tableRuntimes[0].Table, &tables[0])
		require.Len(t, tableRuntimes[0].TransformerContext, 1)
		require.Equal(t, tableRuntimes[0].TransformerContext[0].Transformer, transformerMock)
		require.Nil(t, tableRuntimes[0].TransformerContext[0].Condition)
		require.Nil(t, tableRuntimes[0].Condition)
		require.Equal(t, tableRuntimes[0].Query, tableConfigs[0].Query)
		require.Equal(t, tableRuntimes[0].TableDriver, tableDriverMock)
		tableDriverMock.AssertExpectations(t)
		transformerMock.AssertExpectations(t)
	})

	t.Run("error unknown transformer name", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		tr := registry.NewTransformerRegistry()

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				Transformers: []models.TransformerConfig{
					{
						Name: "UnknownTransformer",
						StaticParams: map[string]models.ParamsValue{
							"column": models.ParamsValue("id"),
						},
					},
				},
			},
		}

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		_, err := p.Build(ctx)
		require.ErrorIs(t, err, models.ErrFatalValidationError)
		require.True(t, vc.IsFatal())
		require.Equal(t, vc.Len(), 1)
		require.Equal(t, vc.GetWarnings()[0].Msg, "transformer is not found")
		tableDriverMock.AssertExpectations(t)
	})

	t.Run("schema validation error", func(t *testing.T) {
		dumpQueries := make([]string, len(tables))
		tableDriverMock := mocks2.NewTableDriverMock()
		tableDriverMock.On("Table").Return(&tables[0])
		tableDriverMock.On("GetColumnByName", "id").
			Return(
				&models.Column{
					Idx:      1,
					Name:     "id",
					TypeName: "int",
					TypeOID:  12,
				},
				nil,
			)
		newDriverFuncMock := func(
			ctx context.Context,
			table models.Table,
			columnsTypeOverride map[string]string,
		) (commonininterfaces.TableDriver, error) {
			return tableDriverMock, nil
		}

		transformerMock, newFunc := mocks2.NewTransformerMock(func(
			ctx context.Context,
			tableDriver commonininterfaces.TableDriver,
			parameters map[string]parameters2.Parameterizer,
		) error {
			assert.Equal(t, tableDriverMock, tableDriver)
			v := utils.Must(parameters["column"].Value())
			require.Equal(t, v, "id")
			return nil
		})

		tr := registry.NewTransformerRegistry()
		tr.MustRegister(
			utils2.NewTransformerDefinition(
				utils2.NewTransformerProperties("TestTransformer", "test desc"),
				newFunc,
				parameters2.MustNewParameterDefinition("column", "test desc").
					SetIsColumn(
						models.NewColumnProperties().
							SetAllowedColumnTypes("int"),
					),
			).SetSchemaValidator(func(
				_ context.Context,
				_ models.Table,
				_ *utils2.TransformerProperties,
				_ map[string]*parameters2.StaticParameter) error {
				return assert.AnError
			}),
		)

		tableConfigs := []models.TableConfig{
			{
				Schema: "public",
				Name:   "test",
				Transformers: []models.TransformerConfig{
					{
						Name: "TestTransformer",
						StaticParams: map[string]models.ParamsValue{
							"column": models.ParamsValue("id"),
						},
					},
				},
			},
		}

		p := New(tables, dumpQueries, tableConfigs, newDriverFuncMock, tr)
		ctx := context.Background()
		vc := validationcollector.NewCollector()
		ctx = validationcollector.WithCollector(ctx, vc)
		_, err := p.Build(ctx)
		require.ErrorIs(t, err, assert.AnError)
		require.False(t, vc.HasWarnings())
		tableDriverMock.AssertExpectations(t)
		transformerMock.AssertExpectations(t)
	})
}
