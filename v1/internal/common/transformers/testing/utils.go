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

package testing

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonrecord "github.com/greenmaskio/greenmask/v1/internal/common/record"
	commontabledriver "github.com/greenmaskio/greenmask/v1/internal/common/tabledriver"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

type TransformerTestEnvReal struct {
	Columns                map[string]commonmodels.Column
	Collector              *validationcollector.Collector
	Definition             *transformerutils.TransformerDefinition
	Transformer            commonininterfaces.Transformer
	StaticParameterValues  map[string]commonmodels.ParamsValue
	DynamicParameterValues map[string]commonmodels.DynamicParamValue
	Ctx                    context.Context
	New                    transformerutils.NewTransformerFunc
	t                      *testing.T
	TableDriverReal        *commontabledriver.TableDriver
	DBMSDriverReal         *mysqldbmsdriver.Driver
	Table                  commonmodels.Table
	Recorder               *commonrecord.Record
	Row                    *DummyRow
	InitializedParameters  map[string]commonparameters.Parameterizer
}

func NewTransformerTestEnvReal(
	t *testing.T,
	def *transformerutils.TransformerDefinition,
	columns []commonmodels.Column,
	staticParams map[string]commonmodels.ParamsValue,
	dynamicParams map[string]commonmodels.DynamicParamValue,
	opt ...func(*TransformerTestEnvReal),
) *TransformerTestEnvReal {
	t.Helper()

	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	columnsMap := make(map[string]commonmodels.Column, len(columns))
	for _, c := range columns {
		if c.Name == "" {
			panic("column name should be provided")
		}
		if c.TypeOID == 0 {
			panic("column type OID should be provided")
		}
		if c.TypeName == "" {
			panic("column type name should be provided")
		}
		columnsMap[c.Name] = c
	}

	table := commonmodels.Table{
		Schema:  "public",
		Name:    "test_table",
		Columns: columns,
	}

	driver := mysqldbmsdriver.New()
	tableDriver, err := commontabledriver.New(ctx, driver, &table, nil)
	require.NoError(t, err)
	require.Empty(t, vc.GetWarnings())

	var transformerNew transformerutils.NewTransformerFunc
	if def != nil {
		transformerNew = def.New
	}
	row := NewDummyRow(len(columns))
	setup := &TransformerTestEnvReal{
		Definition:             def,
		Collector:              vc,
		Ctx:                    ctx,
		New:                    transformerNew,
		t:                      t,
		Columns:                columnsMap,
		Table:                  table,
		TableDriverReal:        tableDriver,
		DBMSDriverReal:         driver,
		Row:                    row,
		Recorder:               commonrecord.NewRecord(row, tableDriver),
		StaticParameterValues:  staticParams,
		DynamicParameterValues: dynamicParams,
	}

	// Allow test-specific overrides
	for _, o := range opt {
		o(setup)
	}

	return setup
}

func (m *TransformerTestEnvReal) InitParameters(t *testing.T, ctx context.Context) error {
	t.Helper()
	var err error
	m.InitializedParameters, err = commonparameters.InitParameters(
		ctx,
		m.TableDriverReal,
		m.Definition.Parameters,
		m.StaticParameterValues,
		m.DynamicParameterValues,
	)
	if err != nil {
		return err
	}
	columnParameters := make(map[string]*commonparameters.StaticParameter)
	for _, p := range m.InitializedParameters {
		if !p.IsDynamic() && p.GetDefinition().IsColumn {
			columnParameters[p.Name()] = p.(*commonparameters.StaticParameter)
		}
	}
	err = m.Definition.ValidateColumnParameters(
		ctx,
		m.Table,
		columnParameters,
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *TransformerTestEnvReal) InitTransformer(
	t *testing.T,
	ctx context.Context,
) error {
	t.Helper()
	var err error
	m.Transformer, err = m.Definition.New(ctx, m.TableDriverReal, m.InitializedParameters)
	if err != nil {
		return fmt.Errorf("create Transformer: %w", err)
	}
	if err := m.Transformer.Init(ctx); err != nil {
		return fmt.Errorf("init Transformer: %w", err)
	}

	return nil
}

// SetRecord - init Recorder with mysql-specific record.
func (m *TransformerTestEnvReal) SetRecord(t *testing.T, record ...*commonmodels.ColumnRawValue) {
	t.Helper()
	m.Row.SetRowRawColumnValue(record)
}

func (m *TransformerTestEnvReal) GetRecord() commonininterfaces.Recorder {
	return m.Recorder
}

func (m *TransformerTestEnvReal) Transform(t *testing.T, ctx context.Context) error {
	t.Helper()
	for _, p := range m.InitializedParameters {
		if !p.IsDynamic() {
			continue
		}
		p.(*commonparameters.DynamicParameter).SetRecord(m.Recorder)
	}
	return m.Transformer.Transform(ctx, m.Recorder)
}
