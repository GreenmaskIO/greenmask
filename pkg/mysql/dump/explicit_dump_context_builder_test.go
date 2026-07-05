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

package dump

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/mocks"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	schemadump "github.com/greenmaskio/greenmask/pkg/mysql/dump/factory/schema"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

// --- test doubles -----------------------------------------------------------

type stubCond struct{}

func (stubCond) Evaluate(core.Recorder) (bool, error) { return true, nil }
func (stubCond) Expression() string                   { return "" }

type stubTransformerContext struct{}

func (stubTransformerContext) SetRecordForDynamicParameters(core.Recorder)    {}
func (stubTransformerContext) EvaluateWhen(core.Recorder) (bool, error)       { return true, nil }
func (stubTransformerContext) Init(context.Context) error                     { return nil }
func (stubTransformerContext) Transform(context.Context, core.Recorder) error { return nil }
func (stubTransformerContext) Done(context.Context) error                     { return nil }
func (stubTransformerContext) GetAffectedColumns() map[int]string             { return nil }
func (stubTransformerContext) Describe() string                               { return "stub" }
func (stubTransformerContext) GetSnapshot(position int) (core.TransformationSnapshot, error) {
	return core.TransformationSnapshot{Name: "stub", Position: position}, nil
}

// stubTableInitDeps is a configurable implementation of tableInitDeps that
// records how it was invoked and returns canned results/errors.
type stubTableInitDeps struct {
	driver     core.TableDriver
	driverErr  error
	cond       core.CondEvaluator
	condErr    error
	transforms []core.TransformerContexter
	transErr   error

	newDriverCalls   int
	compileCalls     int
	initTransCalls   int
	lastOverride     map[string]string
	lastTransConfigs []core.TransformerConfig
}

func (s *stubTableInitDeps) NewTableDriver(
	_ context.Context, _ core.Table, override map[string]string,
) (core.TableDriver, error) {
	s.newDriverCalls++
	s.lastOverride = override
	return s.driver, s.driverErr
}

func (s *stubTableInitDeps) CompileCondition(
	_ context.Context, _ core.Table, _ *core.TableConfig,
) (core.CondEvaluator, error) {
	s.compileCalls++
	return s.cond, s.condErr
}

func (s *stubTableInitDeps) InitTransformers(
	_ context.Context, _ core.TableDriver, configs []core.TransformerConfig, _ core.TransformerRegistry,
) ([]core.TransformerContexter, error) {
	s.initTransCalls++
	s.lastTransConfigs = configs
	return s.transforms, s.transErr
}

// --- helpers ----------------------------------------------------------------

func tableObj(id core.ObjectID, schema, name string) core.Object {
	return core.Object{
		ID:      id,
		Kind:    kinds.ObjectKindTable,
		Name:    name,
		Payload: core.Table{ID: int(id), Schema: schema, Name: name},
	}
}

func tablePayload(t *testing.T, spec core.ObjectDumpSpec) transformercontext.TableDumpContext {
	t.Helper()
	p, ok := spec.Payload.(transformercontext.TableDumpContext)
	require.True(t, ok, "payload must be TableDumpContext, got %T", spec.Payload)
	return p
}

func newIntrospection(objs ...core.Object) core.IntrospectionResult {
	return core.IntrospectionResult{
		Engine: core.DBMSEngineMySQL,
		KindsMap: map[core.ObjectKind][]core.Object{
			kinds.ObjectKindTable: objs,
		},
	}
}

func dbObj(id core.ObjectID, name string) core.Object {
	return core.Object{ID: id, Kind: kinds.ObjectKindDatabase, Name: name}
}

func introspectionWithDatabases(tables, databases []core.Object) core.IntrospectionResult {
	return core.IntrospectionResult{
		Engine: core.DBMSEngineMySQL,
		KindsMap: map[core.ObjectKind][]core.Object{
			kinds.ObjectKindTable:    tables,
			kinds.ObjectKindDatabase: databases,
		},
	}
}

// --- validateSupportedKinds -------------------------------------------------

func TestValidateSupportedKinds(t *testing.T) {
	tests := []struct {
		name    string
		kinds   []core.ObjectKind
		wantErr bool
	}{
		{"table data section", []core.ObjectKind{kinds.ObjectKindTable}, false},
		{"schema sections", []core.ObjectKind{kinds.ObjectKindDatabase}, false},
		{"mixed valid", []core.ObjectKind{kinds.ObjectKindTable}, false},
		{"empty", nil, false},
		{"foreign data section kind", []core.ObjectKind{core.ObjectKind("pg.table")}, true},
		{"unsupported schema section kind", []core.ObjectKind{core.ObjectKind("pg.schema")}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSupportedKinds(tt.kinds)
			if tt.wantErr {
				require.ErrorIs(t, err, errUnsupportedObjectKind)
				return
			}
			require.NoError(t, err)
		})
	}
}

// --- payloadToTableDefinition -----------------------------------------------

func TestPayloadToTableDefinition(t *testing.T) {
	table := core.Table{ID: 7, Schema: "public", Name: "users"}
	tests := []struct {
		name    string
		obj     core.Object
		want    core.Table
		wantErr bool
	}{
		{
			name: "valid",
			obj:  core.Object{Kind: kinds.ObjectKindTable, Payload: table},
			want: table,
		},
		{
			name:    "wrong kind",
			obj:     core.Object{Kind: kinds.ObjectKindDatabase, Payload: core.Table{}},
			wantErr: true,
		},
		{
			name:    "wrong payload type",
			obj:     core.Object{Kind: kinds.ObjectKindTable, Payload: "not a table"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := payloadToTableDefinition(tt.obj)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// --- buildSchemaDumpSpecs ----------------------------------------------------

func TestBuildSchemaDumpSpecs(t *testing.T) {
	t.Run("one pre-data and one post-data spec per database", func(t *testing.T) {
		in := core.ExplicitDumpContextInput{
			IntrospectionResult: newIntrospection(
				tableObj(1, "shop", "users"),
				tableObj(2, "shop", "orders"), // same db -> not duplicated
				tableObj(3, "warehouse", "items"),
			),
		}
		specs, err := NewExplicitDumpContextBuilder(nil).buildSchemaDumpSpecs(context.Background(), in, new(core.TaskIDSequence))
		require.NoError(t, err)

		// 2 databases x 2 sections, grouped section-first. Every spec carries the
		// engine-level kind; section and database distinguish them.
		require.Len(t, specs, 4)
		for _, s := range specs {
			assert.Equal(t, kinds.SchemaObjectKindDatabase, s.Kind)
		}

		assert.Equal(t, []string{"shop", "warehouse", "shop", "warehouse"},
			[]string{
				specs[0].Payload.(schemadump.Payload).Name,
				specs[1].Payload.(schemadump.Payload).Name,
				specs[2].Payload.(schemadump.Payload).Name,
				specs[3].Payload.(schemadump.Payload).Name,
			})
		assert.Equal(t,
			[]core.DumpSection{
				core.DumpSectionPreData, core.DumpSectionPreData,
				core.DumpSectionPostData, core.DumpSectionPostData,
			},
			[]core.DumpSection{
				specs[0].Payload.(schemadump.Payload).Section,
				specs[1].Payload.(schemadump.Payload).Section,
				specs[2].Payload.(schemadump.Payload).Section,
				specs[3].Payload.(schemadump.Payload).Section,
			})

		ids := map[core.TaskID]struct{}{}
		for _, s := range specs {
			_, dup := ids[s.TaskID]
			assert.False(t, dup, "duplicate task id %d", s.TaskID)
			ids[s.TaskID] = struct{}{}
		}
	})

	t.Run("respects AllowedObjects when collecting databases", func(t *testing.T) {
		in := core.ExplicitDumpContextInput{
			IntrospectionResult: newIntrospection(
				tableObj(1, "shop", "users"),
				tableObj(2, "warehouse", "items"),
			),
			AllowedObjects: map[core.ObjectKind][]core.ObjectID{
				kinds.ObjectKindTable: {1}, // only the shop table is allowed
			},
		}
		specs, err := NewExplicitDumpContextBuilder(nil).buildSchemaDumpSpecs(context.Background(), in, new(core.TaskIDSequence))
		require.NoError(t, err)
		require.Len(t, specs, 2)
		for _, s := range specs {
			assert.Equal(t, "shop", s.Payload.(schemadump.Payload).Name)
		}
	})

	t.Run("no tables yields no schema specs", func(t *testing.T) {
		specs, err := NewExplicitDumpContextBuilder(nil).buildSchemaDumpSpecs(
			context.Background(), core.ExplicitDumpContextInput{}, new(core.TaskIDSequence))
		require.NoError(t, err)
		assert.Empty(t, specs)
	})

	t.Run("resolves ObjectID from introspected database objects", func(t *testing.T) {
		in := core.ExplicitDumpContextInput{
			IntrospectionResult: introspectionWithDatabases(
				[]core.Object{tableObj(1, "shop", "users"), tableObj(2, "warehouse", "items")},
				[]core.Object{dbObj(10, "shop"), dbObj(11, "warehouse")},
			),
		}
		specs, err := NewExplicitDumpContextBuilder(nil).buildSchemaDumpSpecs(context.Background(), in, new(core.TaskIDSequence))
		require.NoError(t, err)
		require.Len(t, specs, 4)

		byName := map[string]core.ObjectID{}
		for _, s := range specs {
			byName[s.Payload.(schemadump.Payload).Name] = s.ObjectID
		}
		assert.Equal(t, core.ObjectID(10), byName["shop"])
		assert.Equal(t, core.ObjectID(11), byName["warehouse"])
	})
}

// --- initTable --------------------------------------------------------------

func TestInitTable(t *testing.T) {
	sentinel := errors.New("boom")
	driverMock := mocks.NewTableDriverMock()
	transforms := []core.TransformerContexter{stubTransformerContext{}}

	withTransformers := &core.TableConfig{
		Schema:              "public",
		Name:                "users",
		ColumnsTypeOverride: map[string]string{"id": "text"},
		Transformers:        []core.TransformerConfig{{Name: "Hash"}},
	}

	tests := []struct {
		name        string
		cfg         *core.TableConfig
		subsetQuery string
		obj         core.Object
		deps        *stubTableInitDeps
		wantErr     error // matched with ErrorIs; nil means no error expected
		wantErrAny  bool  // expect any error (no sentinel to match)
		// assert runs on success only.
		assert func(t *testing.T, spec core.ObjectDumpSpec, deps *stubTableInitDeps)
	}{
		{
			name:        "no config: raw dump, no driver, no condition",
			cfg:         nil,
			subsetQuery: "WHERE id > 1",
			obj:         tableObj(1, "public", "users"),
			deps:        &stubTableInitDeps{},
			assert: func(t *testing.T, spec core.ObjectDumpSpec, deps *stubTableInitDeps) {
				assert.Equal(t, core.DumpModeRaw, spec.Mode)
				assert.Equal(t, kinds.ObjectKindTable, spec.Kind)
				assert.Equal(t, core.ObjectID(1), spec.ObjectID)
				assert.Equal(t, "users", spec.Name)

				p := tablePayload(t, spec)
				assert.Equal(t, "WHERE id > 1", p.Query)
				assert.Nil(t, p.TableDriver)
				assert.Nil(t, p.Condition)
				assert.Nil(t, p.TransformerContext)

				// A driver is only needed for transformers — nothing runs.
				assert.Zero(t, deps.newDriverCalls)
				assert.Zero(t, deps.compileCalls)
				assert.Zero(t, deps.initTransCalls)
			},
		},
		{
			name:        "config without transformers: raw dump, condition honoured, no driver",
			cfg:         &core.TableConfig{Schema: "public", Name: "users"},
			subsetQuery: "WHERE active",
			obj:         tableObj(2, "public", "users"),
			deps:        &stubTableInitDeps{cond: stubCond{}},
			assert: func(t *testing.T, spec core.ObjectDumpSpec, deps *stubTableInitDeps) {
				assert.Equal(t, core.DumpModeRaw, spec.Mode)
				p := tablePayload(t, spec)
				assert.Equal(t, "WHERE active", p.Query)
				assert.NotNil(t, p.Condition, "table condition is honoured even in raw mode")
				assert.Nil(t, p.TableDriver)

				assert.Equal(t, 1, deps.compileCalls)
				assert.Zero(t, deps.newDriverCalls, "driver must not be built without transformers")
				assert.Zero(t, deps.initTransCalls)
			},
		},
		{
			name:        "config query used when subset query is empty",
			cfg:         &core.TableConfig{Schema: "public", Name: "users", Query: "SELECT * FROM users"},
			subsetQuery: "",
			obj:         tableObj(3, "public", "users"),
			deps:        &stubTableInitDeps{},
			assert: func(t *testing.T, spec core.ObjectDumpSpec, _ *stubTableInitDeps) {
				assert.Equal(t, "SELECT * FROM users", tablePayload(t, spec).Query)
			},
		},
		{
			name:        "subset query wins over config query",
			cfg:         &core.TableConfig{Schema: "public", Name: "users", Query: "SELECT * FROM users"},
			subsetQuery: "WHERE id IN (1,2)",
			obj:         tableObj(3, "public", "users"),
			deps:        &stubTableInitDeps{},
			assert: func(t *testing.T, spec core.ObjectDumpSpec, _ *stubTableInitDeps) {
				assert.Equal(t, "WHERE id IN (1,2)", tablePayload(t, spec).Query)
			},
		},
		{
			name: "with transformers: transformed mode, driver and transformers wired",
			cfg:  withTransformers,
			obj:  tableObj(4, "public", "users"),
			deps: &stubTableInitDeps{driver: driverMock, cond: stubCond{}, transforms: transforms},
			assert: func(t *testing.T, spec core.ObjectDumpSpec, deps *stubTableInitDeps) {
				assert.Equal(t, core.DumpModeTransformed, spec.Mode)
				p := tablePayload(t, spec)
				assert.Same(t, driverMock, p.TableDriver)
				assert.Equal(t, transforms, p.TransformerContext)
				assert.NotNil(t, p.Condition)

				assert.Equal(t, 1, deps.newDriverCalls)
				assert.Equal(t, 1, deps.initTransCalls)
				assert.Equal(t, map[string]string{"id": "text"}, deps.lastOverride)
				assert.Equal(t, withTransformers.Transformers, deps.lastTransConfigs)
			},
		},
		{
			name:    "compile condition error is propagated",
			cfg:     withTransformers,
			obj:     tableObj(1, "public", "users"),
			deps:    &stubTableInitDeps{condErr: sentinel},
			wantErr: sentinel,
		},
		{
			name:    "driver build error is propagated",
			cfg:     withTransformers,
			obj:     tableObj(1, "public", "users"),
			deps:    &stubTableInitDeps{driverErr: sentinel},
			wantErr: sentinel,
		},
		{
			name:    "transformer init error is propagated",
			cfg:     withTransformers,
			obj:     tableObj(1, "public", "users"),
			deps:    &stubTableInitDeps{driver: driverMock, transErr: sentinel},
			wantErr: sentinel,
		},
		{
			name:       "bad payload is rejected",
			cfg:        nil,
			obj:        core.Object{ID: 1, Kind: kinds.ObjectKindTable, Name: "x", Payload: "not a table"},
			deps:       &stubTableInitDeps{},
			wantErrAny: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExplicitDumpContextBuilder{deps: tt.deps}
			spec, err := b.initTable(context.Background(), tt.cfg, tt.subsetQuery, tt.obj, nil, new(core.TaskIDSequence), core.CompressionNone)

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.wantErrAny:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				tt.assert(t, spec, tt.deps)
			}
		})
	}
}

// --- BuildDumpContext -------------------------------------------------------

func TestBuildDumpContext(t *testing.T) {
	tests := []struct {
		name       string
		input      core.ExplicitDumpContextInput
		wantErr    error // matched with ErrorIs; nil means no error expected
		wantErrAny bool
		assert     func(t *testing.T, ctx core.DumpContext)
	}{
		{
			name: "happy path: one spec per table plus per-database schema specs, unique task IDs",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: newIntrospection(
					tableObj(1, "public", "users"),
					tableObj(2, "public", "orders"),
				),
			},
			assert: func(t *testing.T, ctx core.DumpContext) {
				require.Len(t, ctx.DumpObjectSpecs, 2)
				assert.Equal(t, "users", ctx.DumpObjectSpecs[0].Name)
				assert.Equal(t, "orders", ctx.DumpObjectSpecs[1].Name)
				// One database ("public") -> pre-data + post-data schema specs.
				require.Len(t, ctx.SchemaDumpSpecs, 2)
				assert.Equal(t, "public", ctx.SchemaDumpSpecs[0].Payload.(schemadump.Payload).Name)
				assert.Equal(t, kinds.SchemaObjectKindDatabase, ctx.SchemaDumpSpecs[0].Kind)
				assert.Equal(t, core.DumpSectionPreData, ctx.SchemaDumpSpecs[0].Payload.(schemadump.Payload).Section)
				assert.Equal(t, kinds.SchemaObjectKindDatabase, ctx.SchemaDumpSpecs[1].Kind)
				assert.Equal(t, core.DumpSectionPostData, ctx.SchemaDumpSpecs[1].Payload.(schemadump.Payload).Section)

				seen := map[core.TaskID]struct{}{}
				for _, s := range ctx.DumpObjectSpecs {
					_, dup := seen[s.TaskID]
					assert.False(t, dup, "duplicate task id %d", s.TaskID)
					seen[s.TaskID] = struct{}{}
				}
				for _, s := range ctx.SchemaDumpSpecs {
					_, dup := seen[s.TaskID]
					assert.False(t, dup, "duplicate task id %d", s.TaskID)
					seen[s.TaskID] = struct{}{}
				}
			},
		},
		{
			name: "AllowedObjects restricts the dumped tables",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: newIntrospection(
					tableObj(1, "public", "users"),
					tableObj(2, "public", "orders"),
					tableObj(3, "public", "audit"),
				),
				AllowedObjects: map[core.ObjectKind][]core.ObjectID{
					kinds.ObjectKindTable: {2},
				},
			},
			assert: func(t *testing.T, ctx core.DumpContext) {
				require.Len(t, ctx.DumpObjectSpecs, 1)
				assert.Equal(t, "orders", ctx.DumpObjectSpecs[0].Name)
				assert.Equal(t, core.ObjectID(2), ctx.DumpObjectSpecs[0].ObjectID)
			},
		},
		{
			name: "empty AllowedObjects means all tables",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: newIntrospection(
					tableObj(1, "public", "users"),
					tableObj(2, "public", "orders"),
				),
				AllowedObjects: map[core.ObjectKind][]core.ObjectID{},
			},
			assert: func(t *testing.T, ctx core.DumpContext) {
				assert.Len(t, ctx.DumpObjectSpecs, 2)
			},
		},
		{
			name: "no table kind: neither object nor schema specs",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: core.IntrospectionResult{
					Engine:   core.DBMSEngineMySQL,
					KindsMap: map[core.ObjectKind][]core.Object{},
				},
			},
			assert: func(t *testing.T, ctx core.DumpContext) {
				assert.Empty(t, ctx.DumpObjectSpecs)
				assert.Empty(t, ctx.SchemaDumpSpecs, "no databases in scope -> no schema specs")
			},
		},
		{
			name: "unsupported object kind is rejected",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: core.IntrospectionResult{
					Engine: core.DBMSEngineMySQL,
					KindsMap: map[core.ObjectKind][]core.Object{
						core.ObjectKind("pg.table"): {},
					},
				},
			},
			wantErr: errUnsupportedObjectKind,
		},
		{
			name: "initTable error is propagated",
			input: core.ExplicitDumpContextInput{
				IntrospectionResult: newIntrospection(
					core.Object{ID: 1, Kind: kinds.ObjectKindTable, Name: "x", Payload: "not a table"},
				),
			},
			wantErrAny: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &ExplicitDumpContextBuilder{deps: &stubTableInitDeps{}}
			ctx, err := b.BuildDumpContext(context.Background(), tt.input)

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.wantErrAny:
				require.Error(t, err)
			default:
				require.NoError(t, err)
				tt.assert(t, ctx)
			}
		})
	}
}
