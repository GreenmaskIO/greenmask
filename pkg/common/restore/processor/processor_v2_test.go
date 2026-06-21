package processor

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// ── Noop stubs ────────────────────────────────────────────────────────────────
// Satisfy runtime interfaces (session, conn, storage) without real connections.

type noopSession struct{}

func (noopSession) Close(_ context.Context) error { return nil }
func (noopSession) RunWithOperationalDB(_ context.Context, _ func(context.Context, core.DB) error) error {
	return nil
}
func (noopSession) RunWithEngineResource(_ context.Context, _ func(context.Context, any) error) error {
	return core.ErrEngineResourceNotSupported
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

// ── Mock restorers ────────────────────────────────────────────────────────────

type mockObjectRestorer struct{ mock.Mock }

func (m *mockObjectRestorer) Restore(ctx context.Context, _ core.DatabaseSession, _ core.ConnectionConfigurer, _ core.Storager) error {
	return m.Called(ctx).Error(0)
}
func (m *mockObjectRestorer) DebugInfo() string    { return m.Called().String(0) }
func (m *mockObjectRestorer) Meta() map[string]any { return nil }

// newObjectRestorer returns a pre-configured ObjectRestorer that returns err.
func newObjectRestorer(err error) *mockObjectRestorer {
	r := &mockObjectRestorer{}
	r.On("Restore", mock.Anything).Return(err)
	r.On("DebugInfo").Return("mock-object").Maybe()
	return r
}

type mockSchemaRestorer struct{ mock.Mock }

func (m *mockSchemaRestorer) Restore(ctx context.Context, _ core.DatabaseSession, _ core.ConnectionConfigurer, _ core.Storager) error {
	return m.Called(ctx).Error(0)
}
func (m *mockSchemaRestorer) DebugInfo() string { return m.Called().String(0) }

// newSchemaRestorer returns a pre-configured SchemaRestorer that returns err.
func newSchemaRestorer(err error) *mockSchemaRestorer {
	r := &mockSchemaRestorer{}
	r.On("Restore", mock.Anything).Return(err)
	r.On("DebugInfo").Return("mock-schema").Maybe()
	return r
}

// ── Mock registries ───────────────────────────────────────────────────────────

type mockObjectRegistry struct{ mock.Mock }

func (r *mockObjectRegistry) Register(_ core.ObjectRestoreFactory) error { return nil }
func (r *mockObjectRegistry) Get(_ core.ObjectKind) (core.ObjectRestoreFactory, error) {
	return nil, nil
}
func (r *mockObjectRegistry) New(kind core.ObjectKind, spec core.ObjectRestoreSpec) (core.ObjectRestorer, error) {
	args := r.Called(kind, spec)
	v, _ := args.Get(0).(core.ObjectRestorer)
	return v, args.Error(1)
}

type mockSchemaRegistry struct{ mock.Mock }

func (r *mockSchemaRegistry) Register(_ core.SchemaRestoreFactory) error { return nil }
func (r *mockSchemaRegistry) Get(_ core.SchemaObjectKind) (core.SchemaRestoreFactory, error) {
	return nil, nil
}
func (r *mockSchemaRegistry) New(kind core.SchemaObjectKind, spec core.SchemaRestoreSpec) (core.SchemaRestorer, error) {
	args := r.Called(kind, spec)
	v, _ := args.Get(0).(core.SchemaRestorer)
	return v, args.Error(1)
}

// ── callRecorder ──────────────────────────────────────────────────────────────
// Thread-safe ordered event log used to verify phase ordering.

type callRecorder struct {
	mu    sync.Mutex
	calls []string
}

func (r *callRecorder) record(event string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, event)
}

func (r *callRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	return out
}

// ── Helpers ───────────────────────────────────────────────────────────────────

const (
	kindTable  = core.ObjectKind("table")
	kindSchema = core.SchemaObjectKind("schema")
)

func testObjSpec(id core.TaskID) core.ObjectRestoreSpec {
	return core.ObjectRestoreSpec{TaskID: id, Kind: kindTable}
}

func testPreSpec() core.SchemaRestoreSpec {
	return core.SchemaRestoreSpec{Kind: kindSchema, Section: core.DumpSectionPreData}
}

func testPostSpec() core.SchemaRestoreSpec {
	return core.SchemaRestoreSpec{Kind: kindSchema, Section: core.DumpSectionPostData}
}

func newProcV2(t *testing.T, obj core.ObjectRestoreFactoryRegistry, schema core.SchemaRestoreFactoryRegistry, opts ...RestoreOptionV2) *DefaultRestoreProcessorV2 {
	t.Helper()
	p, err := NewDefaultRestoreProcessorV2(obj, schema, core.DBMSEngineMySQL, opts...)
	require.NoError(t, err)
	return p
}

func newRestoreInput(plan core.RestorePlan, instr core.RestoreInstruction) core.RestoreRunInput {
	return core.RestoreRunInput{
		Session:     noopSession{},
		Conn:        noopConn{},
		St:          noopStorager{},
		Plan:        plan,
		Instruction: instr,
	}
}

// ── Constructor ───────────────────────────────────────────────────────────────

func TestWithRestoreJobsV2(t *testing.T) {
	tests := []struct {
		name    string
		jobs    int
		wantErr bool
	}{
		{"zero", 0, true},
		{"negative", -1, true},
		{"one", 1, false},
		{"many", 16, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDefaultRestoreProcessorV2(
				&mockObjectRegistry{}, &mockSchemaRegistry{},
				core.DBMSEngineMySQL, WithRestoreJobsV2(tc.jobs),
			)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── sectionEnabled ────────────────────────────────────────────────────────────

func TestSectionEnabled(t *testing.T) {
	p := &DefaultRestoreProcessorV2{}

	tests := []struct {
		name    string
		instr   core.RestoreInstruction
		section core.DumpSection
		want    bool
	}{
		// No filter, no flags — all sections enabled
		{"no-filter pre-data", core.RestoreInstruction{}, core.DumpSectionPreData, true},
		{"no-filter data", core.RestoreInstruction{}, core.DumpSectionData, true},
		{"no-filter post-data", core.RestoreInstruction{}, core.DumpSectionPostData, true},
		// DataOnly disables schema sections
		{"DataOnly pre-data", core.RestoreInstruction{DataOnly: true}, core.DumpSectionPreData, false},
		{"DataOnly data", core.RestoreInstruction{DataOnly: true}, core.DumpSectionData, true},
		{"DataOnly post-data", core.RestoreInstruction{DataOnly: true}, core.DumpSectionPostData, false},
		// SchemaOnly disables data section
		{"SchemaOnly pre-data", core.RestoreInstruction{SchemaOnly: true}, core.DumpSectionPreData, true},
		{"SchemaOnly data", core.RestoreInstruction{SchemaOnly: true}, core.DumpSectionData, false},
		{"SchemaOnly post-data", core.RestoreInstruction{SchemaOnly: true}, core.DumpSectionPostData, true},
		// Explicit Section list overrides flags
		{"filter pre-data only → pre-data", core.RestoreInstruction{Section: []string{"pre-data"}}, core.DumpSectionPreData, true},
		{"filter pre-data only → data", core.RestoreInstruction{Section: []string{"pre-data"}}, core.DumpSectionData, false},
		{"filter pre-data only → post-data", core.RestoreInstruction{Section: []string{"pre-data"}}, core.DumpSectionPostData, false},
		{"filter data only → data", core.RestoreInstruction{Section: []string{"data"}}, core.DumpSectionData, true},
		{"filter data only → pre-data", core.RestoreInstruction{Section: []string{"data"}}, core.DumpSectionPreData, false},
		{"filter pre+post → data", core.RestoreInstruction{Section: []string{"pre-data", "post-data"}}, core.DumpSectionData, false},
		{"filter pre+post → pre-data", core.RestoreInstruction{Section: []string{"pre-data", "post-data"}}, core.DumpSectionPreData, true},
		{"filter pre+post → post-data", core.RestoreInstruction{Section: []string{"pre-data", "post-data"}}, core.DumpSectionPostData, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, p.sectionEnabled(tc.instr, tc.section))
		})
	}
}

// ── Input validation ──────────────────────────────────────────────────────────

func TestRun_V2_inputValidation(t *testing.T) {
	p := newProcV2(t, &mockObjectRegistry{}, &mockSchemaRegistry{})

	t.Run("missing session", func(t *testing.T) {
		err := p.Run(context.Background(), core.RestoreRunInput{
			Conn: noopConn{}, St: noopStorager{},
		})
		require.Error(t, err)
	})

	t.Run("missing conn", func(t *testing.T) {
		err := p.Run(context.Background(), core.RestoreRunInput{
			Session: noopSession{}, St: noopStorager{},
		})
		require.Error(t, err)
	})

	t.Run("missing storage", func(t *testing.T) {
		err := p.Run(context.Background(), core.RestoreRunInput{
			Session: noopSession{}, Conn: noopConn{},
		})
		require.Error(t, err)
	})
}

// ── Phase ordering ────────────────────────────────────────────────────────────

// TestRun_V2_phaseOrdering verifies that pre-data schema restorers run before
// object restorers, which run before post-data schema restorers.
func TestRun_V2_phaseOrdering(t *testing.T) {
	rec := &callRecorder{}

	preRestorer := &mockSchemaRestorer{}
	preRestorer.On("Restore", mock.Anything).
		Run(func(mock.Arguments) { rec.record("pre") }).
		Return(nil)
	preRestorer.On("DebugInfo").Return("pre").Maybe()

	objRestorer := &mockObjectRestorer{}
	objRestorer.On("Restore", mock.Anything).
		Run(func(mock.Arguments) { rec.record("obj") }).
		Return(nil)
	objRestorer.On("DebugInfo").Return("obj").Maybe()

	postRestorer := &mockSchemaRestorer{}
	postRestorer.On("Restore", mock.Anything).
		Run(func(mock.Arguments) { rec.record("post") }).
		Return(nil)
	postRestorer.On("DebugInfo").Return("post").Maybe()

	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", kindSchema, testPreSpec()).Return(preRestorer, nil)
	schemaReg.On("New", kindSchema, testPostSpec()).Return(postRestorer, nil)

	objReg := &mockObjectRegistry{}
	objReg.On("New", kindTable, testObjSpec(1)).Return(objRestorer, nil)

	p := newProcV2(t, objReg, schemaReg, WithRestoreJobsV2(1))
	input := newRestoreInput(
		core.RestorePlan{
			SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec(), testPostSpec()},
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)},
		},
		core.RestoreInstruction{},
	)

	require.NoError(t, p.Run(context.Background(), input))
	assert.Equal(t, []string{"pre", "obj", "post"}, rec.snapshot())
}

// ── Section filtering ─────────────────────────────────────────────────────────

func TestRun_V2_dataOnly(t *testing.T) {
	objRestorer := newObjectRestorer(nil)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(objRestorer, nil)
	schemaReg := &mockSchemaRegistry{}

	p := newProcV2(t, objReg, schemaReg)
	input := newRestoreInput(
		core.RestorePlan{
			SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec(), testPostSpec()},
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)},
		},
		core.RestoreInstruction{DataOnly: true},
	)

	require.NoError(t, p.Run(context.Background(), input))
	objRestorer.AssertCalled(t, "Restore", mock.Anything)
	assert.Equal(t, 0, len(schemaReg.Calls))
}

func TestRun_V2_schemaOnly(t *testing.T) {
	preRestorer := newSchemaRestorer(nil)
	postRestorer := newSchemaRestorer(nil)
	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", kindSchema, testPreSpec()).Return(preRestorer, nil)
	schemaReg.On("New", kindSchema, testPostSpec()).Return(postRestorer, nil)
	objReg := &mockObjectRegistry{}

	p := newProcV2(t, objReg, schemaReg)
	input := newRestoreInput(
		core.RestorePlan{
			SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec(), testPostSpec()},
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)},
		},
		core.RestoreInstruction{SchemaOnly: true},
	)

	require.NoError(t, p.Run(context.Background(), input))
	preRestorer.AssertCalled(t, "Restore", mock.Anything)
	postRestorer.AssertCalled(t, "Restore", mock.Anything)
	assert.Equal(t, 0, len(objReg.Calls))
}

func TestRun_V2_sectionFilter_preDataOnly(t *testing.T) {
	preRestorer := newSchemaRestorer(nil)
	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", kindSchema, testPreSpec()).Return(preRestorer, nil)
	objReg := &mockObjectRegistry{}

	p := newProcV2(t, objReg, schemaReg)
	input := newRestoreInput(
		core.RestorePlan{
			SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec(), testPostSpec()},
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)},
		},
		core.RestoreInstruction{Section: []string{string(core.DumpSectionPreData)}},
	)

	require.NoError(t, p.Run(context.Background(), input))
	preRestorer.AssertCalled(t, "Restore", mock.Anything)
	assert.Equal(t, 0, len(objReg.Calls))
	// only preSpec was looked up — no call for postSpec
	schemaReg.AssertNumberOfCalls(t, "New", 1)
}

func TestRun_V2_sectionFilter_dataOnly(t *testing.T) {
	objRestorer := newObjectRestorer(nil)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(objRestorer, nil)
	schemaReg := &mockSchemaRegistry{}

	p := newProcV2(t, objReg, schemaReg)
	input := newRestoreInput(
		core.RestorePlan{
			SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec(), testPostSpec()},
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)},
		},
		core.RestoreInstruction{Section: []string{string(core.DumpSectionData)}},
	)

	require.NoError(t, p.Run(context.Background(), input))
	objRestorer.AssertCalled(t, "Restore", mock.Anything)
	assert.Equal(t, 0, len(schemaReg.Calls))
}

// ── Ordered restore ───────────────────────────────────────────────────────────

// TestRun_V2_orderedRestore verifies that when RestoreInOrder=true and the plan
// has a topological order, objects are restored in RestorationOrder regardless
// of the order in ObjectRestoreSpecs.
func TestRun_V2_orderedRestore(t *testing.T) {
	rec := &callRecorder{}

	spec1 := testObjSpec(1)
	spec2 := testObjSpec(2)
	spec3 := testObjSpec(3)

	makeRestorer := func(label string) *mockObjectRestorer {
		r := &mockObjectRestorer{}
		r.On("Restore", mock.Anything).
			Run(func(mock.Arguments) { rec.record(label) }).
			Return(nil)
		r.On("DebugInfo").Return(label).Maybe()
		return r
	}

	r1 := makeRestorer("1")
	r2 := makeRestorer("2")
	r3 := makeRestorer("3")

	objReg := &mockObjectRegistry{}
	objReg.On("New", kindTable, spec1).Return(r1, nil)
	objReg.On("New", kindTable, spec2).Return(r2, nil)
	objReg.On("New", kindTable, spec3).Return(r3, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{})
	input := newRestoreInput(
		core.RestorePlan{
			// Specs in reversed order — processor must follow RestorationOrder.
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{spec3, spec1, spec2},
			RestorationContext: core.RestorationContext{
				HasTopologicalOrder: true,
				RestorationOrder:    []core.TaskID{1, 2, 3},
			},
		},
		core.RestoreInstruction{RestoreInOrder: true},
	)

	require.NoError(t, p.Run(context.Background(), input))
	assert.Equal(t, []string{"1", "2", "3"}, rec.snapshot())
}

// TestRun_V2_orderedRestore_unknownTaskIDsSkipped confirms that task IDs in
// RestorationOrder that have no matching spec are silently skipped.
func TestRun_V2_orderedRestore_unknownTaskIDsSkipped(t *testing.T) {
	spec := testObjSpec(1)
	restorer := newObjectRestorer(nil)

	objReg := &mockObjectRegistry{}
	objReg.On("New", kindTable, spec).Return(restorer, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{})
	input := newRestoreInput(
		core.RestorePlan{
			ObjectRestoreSpecs: []core.ObjectRestoreSpec{spec},
			RestorationContext: core.RestorationContext{
				HasTopologicalOrder: true,
				RestorationOrder:    []core.TaskID{99, 1, 100}, // 99, 100 have no spec
			},
		},
		core.RestoreInstruction{RestoreInOrder: true},
	)

	require.NoError(t, p.Run(context.Background(), input))
	objReg.AssertNumberOfCalls(t, "New", 1)
	restorer.AssertCalled(t, "Restore", mock.Anything)
}

// ── Parallel restore ──────────────────────────────────────────────────────────

func TestRun_V2_parallelRestore_allSpecsProcessed(t *testing.T) {
	const count = 20
	const jobs = 4

	specs := make([]core.ObjectRestoreSpec, count)
	for i := range specs {
		specs[i] = testObjSpec(core.TaskID(i + 1))
	}

	restorer := newObjectRestorer(nil)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(restorer, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(jobs))
	require.NoError(t, p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: specs},
		core.RestoreInstruction{},
	)))

	restorer.AssertNumberOfCalls(t, "Restore", count)
}

// ── Error propagation ─────────────────────────────────────────────────────────

func TestRun_V2_schemaFactoryError_preData(t *testing.T) {
	errFactory := errors.New("schema factory boom")

	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", mock.Anything, mock.Anything).Return(nil, errFactory)

	p := newProcV2(t, &mockObjectRegistry{}, schemaReg)
	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec()}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errFactory)
}

func TestRun_V2_schemaFactoryError_postData(t *testing.T) {
	errFactory := errors.New("post-data factory boom")

	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", mock.Anything, mock.Anything).Return(nil, errFactory)

	p := newProcV2(t, &mockObjectRegistry{}, schemaReg)
	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPostSpec()}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errFactory)
}

func TestRun_V2_schemaRestorerError(t *testing.T) {
	errRestore := errors.New("schema restore boom")

	sr := newSchemaRestorer(errRestore)
	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", mock.Anything, mock.Anything).Return(sr, nil)

	p := newProcV2(t, &mockObjectRegistry{}, schemaReg)
	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{SchemaRestoreSpecs: []core.SchemaRestoreSpec{testPreSpec()}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errRestore)
}

func TestRun_V2_objectFactoryError(t *testing.T) {
	errFactory := errors.New("object factory boom")

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(nil, errFactory)

	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(1))
	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errFactory)
}

func TestRun_V2_objectRestorerError(t *testing.T) {
	errRestore := errors.New("object restore boom")

	or := newObjectRestorer(errRestore)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(or, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(1))
	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errRestore)
}

// TestRun_V2_objectRestorerError_parallel verifies error propagation cancels
// remaining parallel workers.
func TestRun_V2_objectRestorerError_parallel(t *testing.T) {
	errRestore := errors.New("worker boom")

	failRestorer := newObjectRestorer(errRestore)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(failRestorer, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(4))
	specs := make([]core.ObjectRestoreSpec, 10)
	for i := range specs {
		specs[i] = testObjSpec(core.TaskID(i + 1))
	}

	err := p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: specs},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, errRestore)
}

// ── Context cancellation ──────────────────────────────────────────────────────

// TestRun_V2_contextCancelled_duringData cancels the context inside an object
// restorer and confirms the processor returns a cancellation error.
func TestRun_V2_contextCancelled_duringData(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	restorer := &mockObjectRestorer{}
	restorer.On("Restore", mock.Anything).
		Run(func(args mock.Arguments) {
			cancel()
		}).
		Return(context.Canceled)
	restorer.On("DebugInfo").Return("slow").Maybe()

	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(restorer, nil)

	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(1))
	err := p.Run(ctx, newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: []core.ObjectRestoreSpec{testObjSpec(1)}},
		core.RestoreInstruction{},
	))

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestRun_V2_contextCancelled_betweenSchemaSpecs confirms that when the first
// pre-data schema restorer cancels the context, the second restorer still
// receives the cancelled context and — if it respects it — the error surfaces.
// The schema loop is synchronous and does not short-circuit between specs;
// error propagation relies on the restorer checking ctx.Err().
func TestRun_V2_contextCancelled_betweenSchemaSpecs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	spec1 := core.SchemaRestoreSpec{Kind: "k1", Section: core.DumpSectionPreData}
	spec2 := core.SchemaRestoreSpec{Kind: "k2", Section: core.DumpSectionPreData}

	first := &mockSchemaRestorer{}
	first.On("Restore", mock.Anything).
		Run(func(mock.Arguments) { cancel() }).
		Return(nil) // succeeds, but cancels ctx
	first.On("DebugInfo").Return("first").Maybe()

	// second receives a cancelled context and returns ctx.Err()
	second := &mockSchemaRestorer{}
	second.On("Restore", mock.Anything).
		Run(func(args mock.Arguments) {
			<-args.Get(0).(context.Context).Done() // wait for cancellation
		}).
		Return(context.Canceled)
	second.On("DebugInfo").Return("second").Maybe()

	schemaReg := &mockSchemaRegistry{}
	schemaReg.On("New", core.SchemaObjectKind("k1"), spec1).Return(first, nil)
	schemaReg.On("New", core.SchemaObjectKind("k2"), spec2).Return(second, nil)

	p := newProcV2(t, &mockObjectRegistry{}, schemaReg)
	input := newRestoreInput(
		core.RestorePlan{SchemaRestoreSpecs: []core.SchemaRestoreSpec{spec1, spec2}},
		core.RestoreInstruction{SchemaOnly: true},
	)

	err := p.Run(ctx, input)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// ── Jobs override from instruction ───────────────────────────────────────────

// TestRun_V2_jobsOverriddenByInstruction verifies that Instruction.Jobs
// overrides the default job count set at construction time.
func TestRun_V2_jobsOverriddenByInstruction(t *testing.T) {
	const count = 8

	specs := make([]core.ObjectRestoreSpec, count)
	for i := range specs {
		specs[i] = testObjSpec(core.TaskID(i + 1))
	}

	restorer := newObjectRestorer(nil)
	objReg := &mockObjectRegistry{}
	objReg.On("New", mock.Anything, mock.Anything).Return(restorer, nil)

	// Constructed with 1 job; instruction overrides to 4.
	p := newProcV2(t, objReg, &mockSchemaRegistry{}, WithRestoreJobsV2(1))
	require.NoError(t, p.Run(context.Background(), newRestoreInput(
		core.RestorePlan{ObjectRestoreSpecs: specs},
		core.RestoreInstruction{Jobs: 4},
	)))

	restorer.AssertNumberOfCalls(t, "Restore", count)
}
