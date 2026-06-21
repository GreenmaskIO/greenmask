package processor

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ──────────────────────────────────────────────────────────────────────────────
// Mock types
// ──────────────────────────────────────────────────────────────────────────────

// mockObjectDumper implements core.ObjectDumper.
type mockObjectDumper struct{ mock.Mock }

func (m *mockObjectDumper) Dump(ctx context.Context, _ core.DatabaseSession, _ core.Storager) (core.ObjectDumpStat, error) {
	args := m.Called(ctx)
	if args.Error(1) != nil {
		return core.ObjectDumpStat{}, args.Error(1)
	}
	return args.Get(0).(core.ObjectDumpStat), nil
}

func (m *mockObjectDumper) DebugInfo() string    { return m.Called().String(0) }
func (m *mockObjectDumper) Meta() map[string]any { return nil }

// newObjectDumper returns a dumper preconfigured with a fixed stat/err response.
// DebugInfo is set up as optional (Maybe) because the processor calls it in debug
// logs and error formatting without a guaranteed fixed count.
func newObjectDumper(stat core.ObjectDumpStat, err error) *mockObjectDumper {
	d := &mockObjectDumper{}
	d.On("Dump", mock.Anything).Return(stat, err)
	d.On("DebugInfo").Return("mock-object-task").Maybe()
	return d
}

// mockSchemaDumper implements core.SchemaDumper.
type mockSchemaDumper struct{ mock.Mock }

func (m *mockSchemaDumper) Dump(ctx context.Context, _ core.ConnectionConfigurer, _ core.Storager) (core.SchemaDumpStat, error) {
	args := m.Called(ctx)
	if args.Error(1) != nil {
		return core.SchemaDumpStat{}, args.Error(1)
	}
	return args.Get(0).(core.SchemaDumpStat), nil
}

func (m *mockSchemaDumper) DebugInfo() string    { return "mock-schema-task" }
func (m *mockSchemaDumper) Meta() map[string]any { return nil }

// newSchemaDumper returns a schema dumper preconfigured with a fixed stat/err response.
func newSchemaDumper(stat core.SchemaDumpStat, err error) *mockSchemaDumper {
	d := &mockSchemaDumper{}
	d.On("Dump", mock.Anything).Return(stat, err)
	return d
}

// mockObjectRegistry implements core.ObjectDumpFactoryRegistry.
// Register and Get are no-ops — the processor only calls New.
type mockObjectRegistry struct{ mock.Mock }

func (r *mockObjectRegistry) Register(core.DumpFactory[core.ObjectKind, core.ObjectDumpSpec, core.ObjectDumper]) error {
	return nil
}

func (r *mockObjectRegistry) Get(core.ObjectKind) (core.DumpFactory[core.ObjectKind, core.ObjectDumpSpec, core.ObjectDumper], error) {
	return nil, nil
}

func (r *mockObjectRegistry) New(kind core.ObjectKind, spec core.ObjectDumpSpec) (core.ObjectDumper, error) {
	args := r.Called(kind, spec)
	d, _ := args.Get(0).(core.ObjectDumper) // safe: handles untyped nil
	return d, args.Error(1)
}

// mockSchemaRegistry implements core.SchemaDumpFactoryRegistry.
type mockSchemaRegistry struct{ mock.Mock }

func (r *mockSchemaRegistry) Register(core.DumpFactory[core.SchemaObjectKind, core.SchemaDumpSpec, core.SchemaDumper]) error {
	return nil
}

func (r *mockSchemaRegistry) Get(core.SchemaObjectKind) (core.DumpFactory[core.SchemaObjectKind, core.SchemaDumpSpec, core.SchemaDumper], error) {
	return nil, nil
}

func (r *mockSchemaRegistry) New(kind core.SchemaObjectKind, spec core.SchemaDumpSpec) (core.SchemaDumper, error) {
	args := r.Called(kind, spec)
	d, _ := args.Get(0).(core.SchemaDumper)
	return d, args.Error(1)
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────────

func objectSpec(id core.TaskID) core.ObjectDumpSpec {
	return core.ObjectDumpSpec{TaskID: id, Kind: core.ObjectKindTable}
}

func schemaSpec(id core.TaskID) core.SchemaDumpSpec {
	return core.SchemaDumpSpec{TaskID: id, Kind: core.SchemaObjectKindMysqlDatabase}
}

// newProc creates a processor and fails the test on construction error.
func newProc(t *testing.T, obj core.ObjectDumpFactoryRegistry, schema core.SchemaDumpFactoryRegistry, opts ...OptionV2) *DefaultDumpProcessorV2 {
	t.Helper()
	p, err := NewDumpProcessorV2(obj, schema, core.DBMSEnginePostgreSQL, opts...)
	require.NoError(t, err)
	return p
}

// ──────────────────────────────────────────────────────────────────────────────
// Noop stubs satisfying runtime resource interfaces (not exercised by tests)
// ──────────────────────────────────────────────────────────────────────────────

type noopSession struct{}

func (noopSession) Close(_ context.Context) error { return nil }
func (noopSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return nil
}
func (noopSession) RunWithEngineResource(_ context.Context, _ func(context.Context, any) error) error {
	return nil
}

type noopConn struct{}

func (noopConn) ConnectionConfig() any { return nil }

type noopStorager struct{}

func (noopStorager) GetCwd() string  { return "" }
func (noopStorager) Dirname() string { return "" }
func (noopStorager) ListDir(_ context.Context) ([]string, []core.Storager, error) {
	return nil, nil, nil
}
func (noopStorager) GetObject(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (noopStorager) PutObject(_ context.Context, _ string, _ io.Reader) error     { return nil }
func (noopStorager) Delete(_ context.Context, _ ...string) error                  { return nil }
func (noopStorager) DeleteAll(_ context.Context, _ string) error                  { return nil }
func (noopStorager) Exists(_ context.Context, _ string) (bool, error)             { return false, nil }
func (noopStorager) SubStorage(_ string, _ bool) core.Storager                    { return nil }
func (noopStorager) Stat(_ string) (*core.StorageObjectStat, error)               { return nil, nil }
func (noopStorager) Ping(_ context.Context) error                                 { return nil }

// newRunInput wraps plan and instruction with noop runtime stubs so tests can
// call Run without constructing real DB connections.
func newRunInput(plan core.DumpPlan, instruction core.DumpInstruction) core.DumpRunInput {
	return core.DumpRunInput{
		Session:     noopSession{},
		Conn:        noopConn{},
		St:          noopStorager{},
		DumpID:      core.DumpIDLatest,
		Plan:        plan,
		Instruction: instruction,
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// WithJobsV2 option
// ──────────────────────────────────────────────────────────────────────────────

func TestWithJobsV2(t *testing.T) {
	tests := []struct {
		name    string
		jobs    int
		wantErr bool
	}{
		{name: "zero", jobs: 0, wantErr: true},
		{name: "negative", jobs: -1, wantErr: true},
		{name: "one", jobs: 1, wantErr: false},
		{name: "many", jobs: 16, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDumpProcessorV2(
				&mockObjectRegistry{}, &mockSchemaRegistry{},
				core.DBMSEnginePostgreSQL, WithJobsV2(tc.jobs),
			)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Schema dump
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_schemaDump(t *testing.T) {
	errFactory := errors.New("factory error")
	errDump := errors.New("dump error")

	tests := []struct {
		name          string
		specs         []core.SchemaDumpSpec
		setupSchema   func(*mockSchemaRegistry)
		wantErr       string
		wantSchemaNil bool
	}{
		{
			name:          "empty specs → nil SchemaDump",
			specs:         nil,
			setupSchema:   func(*mockSchemaRegistry) {},
			wantSchemaNil: true,
		},
		{
			name:  "single task succeeds",
			specs: []core.SchemaDumpSpec{schemaSpec(1)},
			setupSchema: func(r *mockSchemaRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newSchemaDumper(core.SchemaDumpStat{OriginalSize: 100}, nil), nil)
			},
			wantSchemaNil: false,
		},
		{
			name:  "multiple tasks all succeed",
			specs: []core.SchemaDumpSpec{schemaSpec(1), schemaSpec(2), schemaSpec(3)},
			setupSchema: func(r *mockSchemaRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newSchemaDumper(core.SchemaDumpStat{OriginalSize: 50}, nil), nil)
			},
			wantSchemaNil: false,
		},
		{
			name:  "factory error propagates",
			specs: []core.SchemaDumpSpec{schemaSpec(1)},
			setupSchema: func(r *mockSchemaRegistry) {
				r.On("New", mock.Anything, mock.Anything).Return(nil, errFactory)
			},
			wantErr: "factory error",
		},
		{
			name:  "dump error propagates",
			specs: []core.SchemaDumpSpec{schemaSpec(1)},
			setupSchema: func(r *mockSchemaRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newSchemaDumper(core.SchemaDumpStat{}, errDump), nil)
			},
			wantErr: "dump error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			schemaReg := &mockSchemaRegistry{}
			tc.setupSchema(schemaReg)
			t.Cleanup(func() { schemaReg.AssertExpectations(t) })

			proc := newProc(t, &mockObjectRegistry{}, schemaReg)
			meta, err := proc.Run(context.Background(), newRunInput(core.DumpPlan{SchemaDumpSpecs: tc.specs}, core.DumpInstruction{}))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.wantSchemaNil {
				assert.Nil(t, meta.SchemaDump)
			} else {
				assert.NotNil(t, meta.SchemaDump)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Data dump
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_dataDump(t *testing.T) {
	errFactory := errors.New("factory error")
	errDump := errors.New("dump error")

	tests := []struct {
		name        string
		specs       []core.ObjectDumpSpec
		setupObject func(*mockObjectRegistry)
		jobs        int
		wantErr     string
		wantDataNil bool
	}{
		{
			name:        "empty specs → nil DataDump",
			specs:       nil,
			setupObject: func(*mockObjectRegistry) {},
			jobs:        1,
			wantDataNil: true,
		},
		{
			name:  "single task single worker succeeds",
			specs: []core.ObjectDumpSpec{objectSpec(1)},
			setupObject: func(r *mockObjectRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newObjectDumper(core.ObjectDumpStat{
						ID: 1, ObjectStat: core.DumpedObjectStat{ID: 1, OriginalSize: 1},
					}, nil), nil)
			},
			jobs:        1,
			wantDataNil: false,
		},
		{
			name:  "multiple tasks single worker — all complete",
			specs: []core.ObjectDumpSpec{objectSpec(1), objectSpec(2), objectSpec(3)},
			setupObject: func(r *mockObjectRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newObjectDumper(core.ObjectDumpStat{ObjectStat: core.DumpedObjectStat{OriginalSize: 1}}, nil), nil)
			},
			jobs:        1,
			wantDataNil: false,
		},
		{
			name:  "multiple tasks multiple workers — all complete",
			specs: []core.ObjectDumpSpec{objectSpec(1), objectSpec(2), objectSpec(3), objectSpec(4)},
			setupObject: func(r *mockObjectRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newObjectDumper(core.ObjectDumpStat{ObjectStat: core.DumpedObjectStat{OriginalSize: 1}}, nil), nil)
			},
			jobs:        4,
			wantDataNil: false,
		},
		{
			name:  "factory error propagates",
			specs: []core.ObjectDumpSpec{objectSpec(1)},
			setupObject: func(r *mockObjectRegistry) {
				r.On("New", mock.Anything, mock.Anything).Return(nil, errFactory)
			},
			jobs:    1,
			wantErr: "factory error",
		},
		{
			name:  "dump error propagates",
			specs: []core.ObjectDumpSpec{objectSpec(1)},
			setupObject: func(r *mockObjectRegistry) {
				r.On("New", mock.Anything, mock.Anything).
					Return(newObjectDumper(core.ObjectDumpStat{}, errDump), nil)
			},
			jobs:    1,
			wantErr: "dump error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objReg := &mockObjectRegistry{}
			tc.setupObject(objReg)
			t.Cleanup(func() { objReg.AssertExpectations(t) })

			proc := newProc(t, objReg, &mockSchemaRegistry{}, WithJobsV2(tc.jobs))
			meta, err := proc.Run(context.Background(), newRunInput(core.DumpPlan{DumpObjectSpecs: tc.specs}, core.DumpInstruction{}))

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.wantDataNil {
				assert.Nil(t, meta.DataDump)
			} else {
				assert.NotNil(t, meta.DataDump)
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Parallel workers: every task is dumped exactly once
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_parallelWorkers_allTasksDumped(t *testing.T) {
	const taskCount = 20
	const jobs = 4

	specs := make([]core.ObjectDumpSpec, taskCount)
	for i := range specs {
		specs[i] = objectSpec(core.TaskID(i + 1))
	}

	// A single shared dumper; the factory returns it for every spec.
	dumper := newObjectDumper(core.ObjectDumpStat{ObjectStat: core.DumpedObjectStat{OriginalSize: 1}}, nil)

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(dumper, nil)

	proc := newProc(t, objReg, &mockSchemaRegistry{}, WithJobsV2(jobs))
	_, err := proc.Run(context.Background(), newRunInput(core.DumpPlan{DumpObjectSpecs: specs}, core.DumpInstruction{}))
	require.NoError(t, err)

	dumper.AssertNumberOfCalls(t, "Dump", taskCount)
}

// ──────────────────────────────────────────────────────────────────────────────
// Context cancelled between schema tasks
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_contextCancelled_betweenSchemaTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// firstDumper cancels the context when Dump is called; second must not run.
	firstDumper := &mockSchemaDumper{}
	firstDumper.On("Dump", mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(core.SchemaDumpStat{}, nil)

	secondDumper := &mockSchemaDumper{}
	// No Dump expectation — panics if called, which makes the test self-verifying.

	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", mock.Anything, mock.Anything).Return(firstDumper, nil).Once()
	schemaReg.On("New", mock.Anything, mock.Anything).Return(secondDumper, nil).Once()

	proc := newProc(t, &mockObjectRegistry{}, schemaReg)
	_, err := proc.Run(ctx, newRunInput(core.DumpPlan{
		SchemaDumpSpecs: []core.SchemaDumpSpec{schemaSpec(1), schemaSpec(2)},
	}, core.DumpInstruction{}))

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	secondDumper.AssertNotCalled(t, "Dump")
}

// ──────────────────────────────────────────────────────────────────────────────
// Context cancelled during data dump
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_contextCancelled_duringDataDump(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	dumper := &mockObjectDumper{}
	dumper.On("DebugInfo").Return("slow-task").Maybe()
	dumper.On("Dump", mock.Anything).
		Run(func(args mock.Arguments) {
			// Block until the context is cancelled, then return.
			<-args.Get(0).(context.Context).Done()
		}).
		Return(core.ObjectDumpStat{}, context.Canceled)

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(dumper, nil)

	proc := newProc(t, objReg, &mockSchemaRegistry{}, WithJobsV2(1))

	go func() { time.Sleep(10 * time.Millisecond); cancel() }()

	_, err := proc.Run(ctx, newRunInput(core.DumpPlan{DumpObjectSpecs: []core.ObjectDumpSpec{objectSpec(1)}}, core.DumpInstruction{}))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// ──────────────────────────────────────────────────────────────────────────────
// Metadata fields are populated correctly
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_metadataFields(t *testing.T) {
	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", mock.Anything, mock.Anything).
		Return(newSchemaDumper(core.SchemaDumpStat{OriginalSize: 200, CompressedSize: 100}, nil), nil)

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).
		Return(newObjectDumper(core.ObjectDumpStat{
			ID:         2,
			ObjectStat: core.DumpedObjectStat{ID: 2, OriginalSize: 500, CompressedSize: 250},
		}, nil), nil)

	p, err := NewDumpProcessorV2(objReg, schemaReg, core.DBMSEngineMySQL)
	require.NoError(t, err)

	plan := core.DumpPlan{
		SchemaDumpSpecs:  []core.SchemaDumpSpec{schemaSpec(1)},
		DumpObjectSpecs:  []core.ObjectDumpSpec{objectSpec(2)},
		Description:      "test dump",
		Tags:             []string{"smoke", "ci"},
		MatchedDatabases: []string{"testdb"},
	}

	before := time.Now()
	meta, err := p.Run(context.Background(), newRunInput(plan, core.DumpInstruction{}))
	after := time.Now()

	require.NoError(t, err)
	assert.Equal(t, core.DBMSEngineMySQL, meta.Engine)
	assert.Equal(t, "test dump", meta.Description)
	assert.Equal(t, []string{"smoke", "ci"}, meta.Tags)
	assert.Equal(t, []string{"testdb"}, meta.Databases)
	assert.False(t, meta.StartedAt.Before(before))
	assert.False(t, meta.StartedAt.After(after))
	assert.False(t, meta.CompletedAt.Before(meta.StartedAt))
	assert.EqualValues(t, 700, meta.OriginalSize)   // 200 (schema) + 500 (data)
	assert.EqualValues(t, 350, meta.CompressedSize) // 100 + 250
	assert.NotNil(t, meta.SchemaDump)
	assert.NotNil(t, meta.DataDump)
}

// ──────────────────────────────────────────────────────────────────────────────
// Processor is reusable across multiple Run calls
// ──────────────────────────────────────────────────────────────────────────────

func TestRun_processorIsReusable(t *testing.T) {
	dumper := newObjectDumper(core.ObjectDumpStat{
		ID:         1,
		ObjectStat: core.DumpedObjectStat{ID: 1, OriginalSize: 1},
	}, nil)

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(dumper, nil)

	proc := newProc(t, objReg, &mockSchemaRegistry{})
	plan := core.DumpPlan{DumpObjectSpecs: []core.ObjectDumpSpec{objectSpec(1)}}

	_, err := proc.Run(context.Background(), newRunInput(plan, core.DumpInstruction{}))
	require.NoError(t, err)

	_, err = proc.Run(context.Background(), newRunInput(plan, core.DumpInstruction{}))
	require.NoError(t, err)

	// New and Dump each called once per Run.
	objReg.AssertNumberOfCalls(t, "New", 2)
	dumper.AssertNumberOfCalls(t, "Dump", 2)
}
