package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	transformerutils "github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type transformerTestEnv struct {
	columns     map[string]commonmodels.Column
	tableDriver *mocks.TableDriverMock
	collector   *validationcollector.Collector
	transformer commonininterfaces.Transformer
	parameters  map[string]*mocks.ParametrizerMock
	recorder    *mocks.RecorderMock
	ctx         context.Context
	new         transformerutils.NewTransformerFunc
	t           *testing.T
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
