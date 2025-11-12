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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonrecord "github.com/greenmaskio/greenmask/v1/internal/common/record"
	commontabledriver "github.com/greenmaskio/greenmask/v1/internal/common/tabledriver"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

type transformerTestEnv struct {
	columns         map[string]commonmodels.Column
	tableDriver     *mocks.TableDriverMock
	collector       *validationcollector.Collector
	transformer     commonininterfaces.Transformer
	parameters      map[string]*mocks.ParametrizerMock
	recorder        *mocks.RecorderMock
	ctx             context.Context
	new             transformerutils.NewTransformerFunc
	t               *testing.T
	tableDriverReal *commontabledriver.TableDriver
	dbmsDriverReal  *mysqldbmsdriver.Driver
	table           commonmodels.Table
}

func (m *transformerTestEnv) getColumnPtr(name string) *commonmodels.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return utils.New(col)
}

func (m *transformerTestEnv) getColumn(name string) commonmodels.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return col
}

func (m *transformerTestEnv) transform() error {
	return m.transformer.Transform(m.ctx, m.recorder)
}

func (m *transformerTestEnv) assertExpectations(t *testing.T) {
	m.tableDriver.AssertExpectations(t)
	m.recorder.AssertExpectations(t)
	for _, p := range m.parameters {
		p.AssertExpectations(t)
	}
}

func withParameter(
	name string,
	setupFn func(parameterMock *mocks.ParametrizerMock, e *transformerTestEnv),
) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		parameterMock, ok := e.parameters[name]
		if !ok {
			parameterMock = mocks.NewParametrizerMock()
		}
		setupFn(parameterMock, e)
		if e.parameters == nil {
			e.parameters = make(map[string]*mocks.ParametrizerMock)
		}
		e.parameters[name] = parameterMock
	}
}

func withRecorder(setupFn func(recorder *mocks.RecorderMock, env *transformerTestEnv)) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		setupFn(e.recorder, e)
	}
}

func withParametersScanner(parameters map[string]any) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		for paramName, paramValue := range parameters {
			withParameter(paramName, func(param *mocks.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(e.t, utils.ScanPointer(paramValue, dest))
					}).Return(nil)
			})(e)
		}
	}
}

func withColumns(columns ...commonmodels.Column) func(*transformerTestEnv) {
	if len(columns) == 0 {
		panic("at least one column should be provided")
	}
	return func(e *transformerTestEnv) {
		if e.columns == nil {
			e.columns = make(map[string]commonmodels.Column)
		}
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
			e.columns[c.Name] = c
		}

		e.table = commonmodels.Table{
			Schema:  "public",
			Name:    "test_table",
			Columns: columns,
		}

		driver := mysqldbmsdriver.New()
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		tableDriver, err := commontabledriver.New(ctx, driver, &e.table, nil)
		require.NoError(e.t, err)
		require.Empty(e.t, vc.GetWarnings())

		e.dbmsDriverReal = driver
		e.tableDriverReal = tableDriver
	}
}

func withContext(fn func(ctx context.Context, env *transformerTestEnv) context.Context) func(*transformerTestEnv) {
	return func(env *transformerTestEnv) {
		ctx := fn(env.ctx, env)
		if ctx != nil {
			env.ctx = ctx
		}
	}
}

func newTransformerTestEnv(
	t *testing.T, new transformerutils.NewTransformerFunc, opt ...func(*transformerTestEnv),
) *transformerTestEnv {
	t.Helper()

	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	setup := &transformerTestEnv{
		tableDriver: mocks.NewTableDriverMock(),
		collector:   vc,
		ctx:         ctx,
		new:         new,
		recorder:    mocks.NewRecorderMock(),
		t:           t,
	}

	// Allow test-specific overrides
	for _, o := range opt {
		o(setup)
	}

	parameters := make(map[string]commonparameters.Parameterizer, len(setup.parameters))
	for k, v := range setup.parameters {
		parameters[k] = v
	}

	var err error
	setup.transformer, err = setup.new(ctx, setup.tableDriver, parameters)
	require.NoError(t, err)

	return setup
}

///////////////

var testNullSeq = []byte("\\N")

type dummyRow struct {
	data []*commonmodels.ColumnRawValue
}

func newDummyRow(numCols int) *dummyRow {
	if numCols <= 0 {
		panic("number of columns should be greater than zero")
	}
	return &dummyRow{data: make([]*commonmodels.ColumnRawValue, numCols)}
}

func (d *dummyRow) GetColumn(idx int) (*commonmodels.ColumnRawValue, error) {
	if idx < 0 || idx >= len(d.data) {
		return nil, errors.New("index out of range")
	}
	return d.data[idx], nil
}

func (d *dummyRow) SetColumn(idx int, v *commonmodels.ColumnRawValue) error {
	if idx < 0 || idx >= len(d.data) {
		return errors.New("index out of range")
	}
	d.data[idx] = v
	return nil
}

func (d *dummyRow) SetRowRawColumnValue(row []*commonmodels.ColumnRawValue) {
	if len(row) != len(d.data) {
		panic("row length does not match")
	}
	for i := range row {
		d.data[i] = row[i]
	}
}

func (d *dummyRow) SetRow(row [][]byte) error {
	panic("implement me")
}

func (d *dummyRow) GetRow() [][]byte {
	panic("implement me")
}

type transformerTestEnvReal struct {
	columns                map[string]commonmodels.Column
	collector              *validationcollector.Collector
	definition             *transformerutils.TransformerDefinition
	transformer            commonininterfaces.Transformer
	staticParameterValues  map[string]commonmodels.ParamsValue
	dynamicParameterValues map[string]commonmodels.DynamicParamValue
	ctx                    context.Context
	new                    transformerutils.NewTransformerFunc
	t                      *testing.T
	tableDriverReal        *commontabledriver.TableDriver
	dbmsDriverReal         *mysqldbmsdriver.Driver
	table                  commonmodels.Table
	recorder               *commonrecord.Record
	row                    *dummyRow
	initializedParameters  map[string]commonparameters.Parameterizer
}

func (m *transformerTestEnvReal) getColumnPtr(name string) *commonmodels.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return utils.New(col)
}

func (m *transformerTestEnvReal) getColumn(name string) commonmodels.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return col
}

func (m *transformerTestEnvReal) transform() error {
	return m.transformer.Transform(m.ctx, m.recorder)
}

func withStaticParametersValue(
	params map[string]commonmodels.ParamsValue,
) func(*transformerTestEnvReal) {
	return func(e *transformerTestEnvReal) {
		e.staticParameterValues = params
	}
}

func withDynamicParametersValue(
	params map[string]commonmodels.DynamicParamValue,
) func(*transformerTestEnvReal) {
	return func(e *transformerTestEnvReal) {
		e.dynamicParameterValues = params
	}
}

func withContextV2(fn func(ctx context.Context, env *transformerTestEnvReal) context.Context) func(*transformerTestEnvReal) {
	return func(env *transformerTestEnvReal) {
		ctx := fn(env.ctx, env)
		if ctx != nil {
			env.ctx = ctx
		}
	}
}

func newTransformerTestEnvReal(
	t *testing.T,
	def *transformerutils.TransformerDefinition,
	columns []commonmodels.Column,
	staticParams map[string]commonmodels.ParamsValue,
	dynamicParams map[string]commonmodels.DynamicParamValue,
	opt ...func(*transformerTestEnvReal),
) *transformerTestEnvReal {
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

	row := newDummyRow(len(columns))
	setup := &transformerTestEnvReal{
		definition:             def,
		collector:              vc,
		ctx:                    ctx,
		new:                    def.New,
		t:                      t,
		columns:                columnsMap,
		table:                  table,
		tableDriverReal:        tableDriver,
		dbmsDriverReal:         driver,
		row:                    row,
		recorder:               commonrecord.NewRecord(row, tableDriver),
		staticParameterValues:  staticParams,
		dynamicParameterValues: dynamicParams,
	}

	// Allow test-specific overrides
	for _, o := range opt {
		o(setup)
	}

	return setup
}

func (m *transformerTestEnvReal) InitParameters(t *testing.T, ctx context.Context) error {
	t.Helper()
	var err error
	m.initializedParameters, err = commonparameters.InitParameters(
		ctx,
		m.tableDriverReal,
		m.definition.Parameters,
		m.staticParameterValues,
		m.dynamicParameterValues,
	)
	if err != nil {
		return err
	}
	columnParameters := make(map[string]*commonparameters.StaticParameter)
	for _, p := range m.initializedParameters {
		if !p.IsDynamic() && p.GetDefinition().IsColumn {
			columnParameters[p.Name()] = p.(*commonparameters.StaticParameter)
		}
	}
	err = m.definition.ValidateColumnParameters(
		ctx,
		m.table,
		columnParameters,
	)
	if err != nil {
		return err
	}

	return nil
}

func (m *transformerTestEnvReal) InitTransformer(
	t *testing.T,
	ctx context.Context,
) error {
	t.Helper()
	var err error
	m.transformer, err = m.definition.New(ctx, m.tableDriverReal, m.initializedParameters)
	if err != nil {
		return fmt.Errorf("create transformer: %w", err)
	}
	if err := m.transformer.Init(ctx); err != nil {
		return fmt.Errorf("init transformer: %w", err)
	}

	return nil
}

func (m *transformerTestEnvReal) DoneTransformer(
	t *testing.T,
	ctx context.Context,
) error {
	t.Helper()
	err := m.transformer.Done(ctx)
	return err
}

// SetRecord - init recorder with mysql-specific record.
func (m *transformerTestEnvReal) SetRecord(t *testing.T, record ...*commonmodels.ColumnRawValue) {
	t.Helper()
	m.row.SetRowRawColumnValue(record)
}

func (m *transformerTestEnvReal) GetRecord() commonininterfaces.Recorder {
	return m.recorder
}

func (m *transformerTestEnvReal) Transform(t *testing.T, ctx context.Context) error {
	t.Helper()
	for _, p := range m.initializedParameters {
		if !p.IsDynamic() {
			continue
		}
		p.(*commonparameters.DynamicParameter).SetRecord(m.recorder)
	}
	return m.transformer.Transform(ctx, m.recorder)
}
