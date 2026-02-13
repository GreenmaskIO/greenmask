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

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	mocks2 "github.com/greenmaskio/greenmask/v1/pkg/common/mocks"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	commonrecord "github.com/greenmaskio/greenmask/v1/pkg/common/record"
	commontabledriver "github.com/greenmaskio/greenmask/v1/pkg/common/tabledriver"
	parameters2 "github.com/greenmaskio/greenmask/v1/pkg/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/pkg/common/transformers/utils"
	utils2 "github.com/greenmaskio/greenmask/v1/pkg/common/utils"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/pkg/mysql/dbmsdriver"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type transformerTestEnv struct {
	columns         map[string]models.Column
	tableDriver     *mocks2.TableDriverMock
	collector       *validationcollector.Collector
	transformer     interfaces.Transformer
	parameters      map[string]*mocks2.ParametrizerMock
	recorder        *mocks2.RecorderMock
	ctx             context.Context
	new             transformerutils.NewTransformerFunc
	t               *testing.T
	tableDriverReal *commontabledriver.TableDriver
	dbmsDriverReal  *mysqldbmsdriver.Driver
	table           models.Table
}

func (m *transformerTestEnv) getColumnPtr(name string) *models.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return utils2.New(col)
}

func (m *transformerTestEnv) getColumn(name string) models.Column {
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
	setupFn func(parameterMock *mocks2.ParametrizerMock, e *transformerTestEnv),
) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		parameterMock, ok := e.parameters[name]
		if !ok {
			parameterMock = mocks2.NewParametrizerMock()
		}
		setupFn(parameterMock, e)
		if e.parameters == nil {
			e.parameters = make(map[string]*mocks2.ParametrizerMock)
		}
		e.parameters[name] = parameterMock
	}
}

func withRecorder(setupFn func(recorder *mocks2.RecorderMock, env *transformerTestEnv)) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		setupFn(e.recorder, e)
	}
}

func withParametersScanner(parameters map[string]any) func(*transformerTestEnv) {
	return func(e *transformerTestEnv) {
		for paramName, paramValue := range parameters {
			withParameter(paramName, func(param *mocks2.ParametrizerMock, env *transformerTestEnv) {
				param.On("Scan", mock.Anything).
					Run(func(args mock.Arguments) {
						dest := args.Get(0)
						require.NoError(e.t, utils2.ScanPointer(paramValue, dest))
					}).Return(nil)
			})(e)
		}
	}
}

func withColumns(columns ...models.Column) func(*transformerTestEnv) {
	if len(columns) == 0 {
		panic("at least one column should be provided")
	}
	return func(e *transformerTestEnv) {
		if e.columns == nil {
			e.columns = make(map[string]models.Column)
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

		e.table = models.Table{
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
		tableDriver: mocks2.NewTableDriverMock(),
		collector:   vc,
		ctx:         ctx,
		new:         new,
		recorder:    mocks2.NewRecorderMock(),
		t:           t,
	}

	// Allow test-specific overrides
	for _, o := range opt {
		o(setup)
	}

	parameters := make(map[string]parameters2.Parameterizer, len(setup.parameters))
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
	data []*models.ColumnRawValue
}

func newDummyRow(numCols int) *dummyRow {
	if numCols <= 0 {
		panic("number of columns should be greater than zero")
	}
	return &dummyRow{data: make([]*models.ColumnRawValue, numCols)}
}

func (d *dummyRow) GetColumn(idx int) (*models.ColumnRawValue, error) {
	if idx < 0 || idx >= len(d.data) {
		return nil, errors.New("index out of range")
	}
	return d.data[idx], nil
}

func (d *dummyRow) SetColumn(idx int, v *models.ColumnRawValue) error {
	if idx < 0 || idx >= len(d.data) {
		return errors.New("index out of range")
	}
	d.data[idx] = v
	return nil
}

func (d *dummyRow) SetRowRawColumnValue(row []*models.ColumnRawValue) {
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
	columns                map[string]models.Column
	collector              *validationcollector.Collector
	definition             *transformerutils.TransformerDefinition
	transformer            interfaces.Transformer
	staticParameterValues  map[string]models.ParamsValue
	dynamicParameterValues map[string]models.DynamicParamValue
	ctx                    context.Context
	new                    transformerutils.NewTransformerFunc
	t                      *testing.T
	tableDriverReal        *commontabledriver.TableDriver
	dbmsDriverReal         *mysqldbmsdriver.Driver
	table                  models.Table
	recorder               *commonrecord.Record
	row                    *dummyRow
	initializedParameters  map[string]parameters2.Parameterizer
}

func (m *transformerTestEnvReal) getColumnPtr(name string) *models.Column {
	col, ok := m.columns[name]
	if !ok {
		panic("column with name " + name + " not found")
	}
	return utils2.New(col)
}

func (m *transformerTestEnvReal) getColumn(name string) models.Column {
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
	params map[string]models.ParamsValue,
) func(*transformerTestEnvReal) {
	return func(e *transformerTestEnvReal) {
		e.staticParameterValues = params
	}
}

func withDynamicParametersValue(
	params map[string]models.DynamicParamValue,
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
	columns []models.Column,
	staticParams map[string]models.ParamsValue,
	dynamicParams map[string]models.DynamicParamValue,
	opt ...func(*transformerTestEnvReal),
) *transformerTestEnvReal {
	t.Helper()

	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	columnsMap := make(map[string]models.Column, len(columns))
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

	table := models.Table{
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
	m.initializedParameters, err = parameters2.InitParameters(
		ctx,
		m.tableDriverReal,
		m.definition.Parameters,
		m.staticParameterValues,
		m.dynamicParameterValues,
	)
	if err != nil {
		return err
	}
	columnParameters := make(map[string]*parameters2.StaticParameter)
	for _, p := range m.initializedParameters {
		if !p.IsDynamic() && p.GetDefinition().IsColumn {
			columnParameters[p.Name()] = p.(*parameters2.StaticParameter)
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
func (m *transformerTestEnvReal) SetRecord(t *testing.T, record ...*models.ColumnRawValue) {
	t.Helper()
	m.row.SetRowRawColumnValue(record)
}

func (m *transformerTestEnvReal) GetRecord() interfaces.Recorder {
	return m.recorder
}

func (m *transformerTestEnvReal) Transform(t *testing.T, ctx context.Context) error {
	t.Helper()
	for _, p := range m.initializedParameters {
		if !p.IsDynamic() {
			continue
		}
		p.(*parameters2.DynamicParameter).SetRecord(m.recorder)
	}
	return m.transformer.Transform(ctx, m.recorder)
}
