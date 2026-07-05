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

package derivation

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// fakeTransformer is a minimal core.Transformer used to populate explicit root
// transformer contexts and to be returned by the fake provisioner for derived
// ones. Its affected column and determinism are fully controllable.
type fakeTransformer struct {
	name          string
	affected      map[int]string
	deterministic bool
}

var _ core.Transformer = (*fakeTransformer)(nil)

func (f *fakeTransformer) Init(context.Context) error                     { return nil }
func (f *fakeTransformer) Done(context.Context) error                     { return nil }
func (f *fakeTransformer) Transform(context.Context, core.Recorder) error { return nil }
func (f *fakeTransformer) GetAffectedColumns() map[int]string             { return f.affected }
func (f *fakeTransformer) Describe() string                               { return f.name }
func (f *fakeTransformer) IsDeterministic() bool                          { return f.deterministic }

// fakeRegistry instantiates a derived transformer context whose affected column
// is read from the cloned config's "column" static parameter — exactly what the
// algorithm retargets.
type fakeRegistry struct{}

func (fakeRegistry) Get(name string) (core.TransformerProvisioner, bool) {
	return fakeProvisioner{name: name}, true
}

type fakeProvisioner struct{ name string }

func (p fakeProvisioner) Init(
	_ context.Context, _ core.TableDriver, cfg core.TransformerConfig,
) (core.TransformerContexter, error) {
	col := string(cfg.StaticParams["column"])
	return &transformercontext.TransformerContext{
		Transformer: &fakeTransformer{
			name:          p.name,
			affected:      map[int]string{0: col},
			deterministic: true,
		},
		// Mirror production stamping (utils/definition.go Init): the cloned config
		// keeps ApplyForReferences, and RandomInt is on the allow-list. This makes
		// the fake faithful so the tests catch re-collection of derived transformers.
		ApplyForReferences:      cfg.ApplyForReferences,
		AllowApplyForReferenced: true,
	}, nil
}

// fakeDriverFactory returns a nil driver; the fake provisioner ignores it.
type fakeDriverFactory struct{}

func (fakeDriverFactory) NewTableDriver(
	context.Context, core.Table, map[string]string,
) (core.TableDriver, error) {
	return nil, nil
}

// errDriverFactory fails to build a driver, exercising the fatal path in
// applyDerived.
type errDriverFactory struct{}

func (errDriverFactory) NewTableDriver(
	context.Context, core.Table, map[string]string,
) (core.TableDriver, error) {
	return nil, errors.New("boom")
}

// rootCtx builds an eligible root transformer context affecting a single
// primary-key column.
func rootCtx(name, col string, applyForRefs, allow, deterministic bool) *transformercontext.TransformerContext {
	return &transformercontext.TransformerContext{
		Transformer:             &fakeTransformer{name: name, affected: map[int]string{0: col}, deterministic: deterministic},
		ApplyForReferences:      applyForRefs,
		AllowApplyForReferenced: allow,
	}
}

// rootCtxMulti builds an eligible root transformer context affecting several
// columns at once.
func rootCtxMulti(name string, affected map[int]string) *transformercontext.TransformerContext {
	return &transformercontext.TransformerContext{
		Transformer:             &fakeTransformer{name: name, affected: affected, deterministic: true},
		ApplyForReferences:      true,
		AllowApplyForReferenced: true,
	}
}

// explicitCtx builds an already-explicit transformer context (no reference
// propagation intent) — used to model "user config wins".
func explicitCtx(name, col string) *transformercontext.TransformerContext {
	return &transformercontext.TransformerContext{
		Transformer: &fakeTransformer{name: name, affected: map[int]string{0: col}, deterministic: true},
	}
}

func tableSpec(
	id core.ObjectID, schema, name string, pk []string,
	transformers []core.TransformerContexter,
) core.ObjectDumpSpec {
	mode := core.DumpModeRaw
	if len(transformers) > 0 {
		mode = core.DumpModeTransformed
	}
	return core.ObjectDumpSpec{
		ObjectID: id,
		Kind:     core.ObjectKind("table"),
		Name:     name,
		Identity: core.EntityIdentity{
			Kind:       "test.table",
			NameParts:  []string{"schema", "table"},
			NameValues: map[string]string{"schema": schema, "table": name},
		},
		Origin: core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
		Mode:   mode,
		Payload: transformercontext.TableDumpContext{
			ColumnKind:         "test.column",
			Table:              &core.Table{Schema: schema, Name: name, PrimaryKey: pk},
			TransformerContext: transformers,
		},
	}
}

// fkEdge builds a child(From) -> parent(To) foreign-key edge with positional
// column alignment RefColumns[i] <-> Columns[i].
func fkEdge(childID, parentID core.ObjectID, constraint string, fkCols, refCols []string) core.ObjectEdge {
	return core.ObjectEdge{
		From: childID,
		To:   parentID,
		Link: core.ObjectLink{
			Kind: core.ObjectLinkKindForeignKey,
			Payload: core.ForeignKeyLinkPayload{
				ConstraintName: constraint,
				Columns:        fkCols,
				RefColumns:     refCols,
			},
		},
	}
}

func graphFrom(edges ...core.ObjectEdge) core.DependencyGraphResult {
	m := map[core.ObjectID][]core.ObjectEdge{}
	for _, e := range edges {
		m[e.From] = append(m[e.From], e)
	}
	return core.DependencyGraphResult{ObjectGraph: core.ObjectGraph{Edges: m}}
}

func tableConfig(schema, name string, transformers ...core.TransformerConfig) core.TableConfig {
	return core.TableConfig{Schema: schema, Name: name, Transformers: transformers}
}

func trCfg(name, col string) core.TransformerConfig {
	return core.TransformerConfig{
		Name:               name,
		ApplyForReferences: true,
		StaticParams:       map[string]core.ParamsValue{"column": core.ParamsValue(col)},
	}
}

// derivedColumns returns the sorted columns carrying a derived transformer on a
// spec (looked up by object id in the result).
func derivedColumns(t *testing.T, ctx core.DumpContext, id core.ObjectID) []string {
	t.Helper()
	var cols []string
	for _, spec := range ctx.DumpObjectSpecs {
		if spec.ObjectID != id {
			continue
		}
		payload := spec.Payload.(transformercontext.TableDumpContext)
		for _, tc := range payload.TransformerContext {
			tctx := tc.(*transformercontext.TransformerContext)
			if tctx.Source.Kind != core.TransformationSourceKindDerived {
				continue
			}
			for _, c := range tctx.GetAffectedColumns() {
				cols = append(cols, c)
			}
		}
	}
	sort.Strings(cols)
	return cols
}

// orderedDerivedColumns returns the derived columns on a spec in their actual
// slice order (NOT sorted), so a test can assert the deterministic append order.
func orderedDerivedColumns(ctx core.DumpContext, id core.ObjectID) []string {
	var cols []string
	for _, spec := range ctx.DumpObjectSpecs {
		if spec.ObjectID != id {
			continue
		}
		payload := spec.Payload.(transformercontext.TableDumpContext)
		for _, tc := range payload.TransformerContext {
			tctx := tc.(*transformercontext.TransformerContext)
			if tctx.Source.Kind != core.TransformationSourceKindDerived {
				continue
			}
			for _, c := range tctx.GetAffectedColumns() {
				cols = append(cols, c)
			}
		}
	}
	return cols
}

func specByID(ctx core.DumpContext, id core.ObjectID) core.ObjectDumpSpec {
	for _, spec := range ctx.DumpObjectSpecs {
		if spec.ObjectID == id {
			return spec
		}
	}
	return core.ObjectDumpSpec{}
}

func TestDerive(t *testing.T) {
	tests := []struct {
		name string
		// deriver overrides the default (fakeRegistry + fakeDriverFactory); used to
		// inject an error-producing driver factory. nil selects the default.
		deriver *Deriver
		in      core.DerivedDumpContextInput
		// wantErr expects Derive to return core.ErrFatalValidationError.
		wantErr bool
		// wantDerived maps object id -> expected derived columns (nil/empty = none).
		wantDerived map[core.ObjectID][]string
		// wantDerivedOrigin lists object ids that must flip to derived origin.
		wantDerivedOrigin []core.ObjectID
		// wantExplicitOrigin lists object ids that must keep explicit origin.
		wantExplicitOrigin []core.ObjectID
	}{
		{
			// orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[RandomInt, apply_for_references]
			// ⇒ orders.user_id inherits RandomInt (user_id is users' PK).
			name: "simple PK to FK propagation",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantDerived:        map[core.ObjectID][]string{2: {"user_id"}},
			wantDerivedOrigin:  []core.ObjectID{2},
			wantExplicitOrigin: []core.ObjectID{1},
		},
		{
			// tablec(c1*,c2*) --FK(c1,c2)--> tableb(b1*,b2*) --FK(b1,b2)--> tablea(a1*,a2*)[RandomInt on a1 & a2]
			// Every FK column is part of the child's PK ⇒ end-to-end chain.
			// ⇒ tableb.{b1,b2} and tablec.{c1,c2} all inherit RandomInt.
			name: "end-to-end composite chain a->b->c",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "tablea", []string{"a1", "a2"},
						[]core.TransformerContexter{
							rootCtx("RandomInt", "a1", true, true, true),
							rootCtx("RandomInt", "a2", true, true, true),
						}),
					tableSpec(2, "app", "tableb", []string{"b1", "b2"}, nil),
					tableSpec(3, "app", "tablec", []string{"c1", "c2"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "tablea",
						trCfg("RandomInt", "a1"), trCfg("RandomInt", "a2")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_b_a", []string{"b1", "b2"}, []string{"a1", "a2"}),
					fkEdge(3, 2, "fk_c_b", []string{"c1", "c2"}, []string{"b1", "b2"}),
				),
			},
			wantDerived: map[core.ObjectID][]string{
				2: {"b1", "b2"},
				3: {"c1", "c2"},
			},
			wantDerivedOrigin: []core.ObjectID{2, 3},
		},
		{
			// items(item_id*, user_id) --FK(user_id)--> orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[RandomInt]
			// orders.user_id is a plain FK, NOT part of orders' PK ⇒ the chain stops
			// at the direct child. ⇒ orders.user_id inherits; items.user_id does NOT.
			name: "non end-to-end stops after direct child",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
					// orders.user_id is a plain FK, NOT part of orders PK.
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
					// items references orders.user_id — but since orders.user_id is not
					// an identifier the chain must not continue to items.
					tableSpec(3, "app", "items", []string{"item_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
					fkEdge(3, 2, "fk_items_orders", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantDerived: map[core.ObjectID][]string{
				2: {"user_id"},
				3: nil,
			},
		},
		{
			// orders(order_id*, user_id)[RandomInt — explicit] --FK(user_id)--> users(user_id*)[RandomInt, apply_for_references]
			// orders already has an explicit RandomInt on user_id ⇒ user config wins,
			// nothing is derived onto it.
			name: "user config wins on child column",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
					// orders already has an explicit RandomInt on user_id.
					tableSpec(2, "app", "orders", []string{"order_id"},
						[]core.TransformerContexter{explicitCtx("RandomInt", "user_id")}),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantDerived:        map[core.ObjectID][]string{2: nil},
			wantExplicitOrigin: []core.ObjectID{2},
		},
		{
			// tablea(id*)[RandomInt] ⇄ tableb(id*)
			//   a.id --FK--> b.id   and   b.id --FK--> a.id   (shared-PK 1:1 cycle)
			// ⇒ tableb.id inherits once; back-propagation to tablea.id hits its own
			// explicit root (user wins); traversal terminates.
			name: "mutual foreign-key cycle terminates without duplicates",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "tablea", []string{"id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "id", true, true, true)}),
					tableSpec(2, "app", "tableb", []string{"id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "tablea", trCfg("RandomInt", "id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(1, 2, "fk_a_b", []string{"id"}, []string{"id"}),
					fkEdge(2, 1, "fk_b_a", []string{"id"}, []string{"id"}),
				),
			},
			// tableb gets exactly one derived on id; tablea keeps only its explicit
			// root (the propagation back to it is skipped as user-config-wins).
			wantDerived: map[core.ObjectID][]string{
				1: nil,
				2: {"id"},
			},
			wantDerivedOrigin:  []core.ObjectID{2},
			wantExplicitOrigin: []core.ObjectID{1},
		},
		{
			// tenants(tenant_id*)[RandomInt, apply_for_references]
			//   ▲                                 ▲
			//   │ FK(tenant_id)                   │ FK(tenant_id)
			// projects(tenant_id*, project_id*)  users(tenant_id*, user_id*)
			//   ▲                                 ▲
			//   │ FK(tenant_id, project_id)       │ FK(tenant_id, user_id)
			//   └──── assignments(tenant_id*, project_id*, user_id*) ────┘
			// assignments.tenant_id is reached via BOTH paths (projects & users)
			// ⇒ RandomInt applied to tenant_id EXACTLY ONCE (dedup), not twice.
			name: "diamond: same root reaches a column via two paths, applied once",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "tenants", []string{"tenant_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "tenant_id", true, true, true)}),
					tableSpec(2, "app", "projects", []string{"tenant_id", "project_id"}, nil),
					tableSpec(3, "app", "users", []string{"tenant_id", "user_id"}, nil),
					tableSpec(4, "app", "assignments", []string{"tenant_id", "project_id", "user_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "tenants", trCfg("RandomInt", "tenant_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_projects_tenants", []string{"tenant_id"}, []string{"tenant_id"}),
					fkEdge(3, 1, "fk_users_tenants", []string{"tenant_id"}, []string{"tenant_id"}),
					fkEdge(4, 2, "fk_assignments_projects",
						[]string{"tenant_id", "project_id"}, []string{"tenant_id", "project_id"}),
					fkEdge(4, 3, "fk_assignments_users",
						[]string{"tenant_id", "user_id"}, []string{"tenant_id", "user_id"}),
				),
			},
			wantDerived: map[core.ObjectID][]string{
				2: {"tenant_id"},
				3: {"tenant_id"},
				// Reached via both fk_assignments_projects and fk_assignments_users,
				// but the same root must produce exactly one derived transformer.
				4: {"tenant_id"},
			},
			wantDerivedOrigin: []core.ObjectID{2, 3, 4},
		},
		{
			// orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[RandomInt engine=random, apply_for_references]
			// A random engine cannot reproduce the same value on both sides of the FK
			// ⇒ fatal, no propagation.
			name: "non-deterministic engine is rejected",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, false)}),
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantErr:     true,
			wantDerived: map[core.ObjectID][]string{2: nil},
		},
		{
			// orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[Cmd, apply_for_references]
			// Cmd is not on the apply_for_references allow-list ⇒ fatal, no propagation.
			name: "transformer not on the allow-list is rejected",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("Cmd", "user_id", true, false, false)}),
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("Cmd", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantErr:     true,
			wantDerived: map[core.ObjectID][]string{2: nil},
		},
		{
			// orders(order_id*, user_id) --FK(id1)--> users(id1*, id2*)[one RandomInt covering BOTH id1 and id2]
			// A transformer affecting multiple columns cannot be retargeted onto a
			// single FK column ⇒ fatal.
			name: "transformer affecting multiple columns is rejected",
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"id1", "id2"},
						[]core.TransformerContexter{rootCtxMulti("RandomInt", map[int]string{0: "id1", 1: "id2"})}),
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "id1")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"id1"}, []string{"id1"}),
				),
			},
			wantErr:     true,
			wantDerived: map[core.ObjectID][]string{2: nil},
		},
		{
			// orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[RandomInt, apply_for_references]
			// The raw child needs a driver, but the driver factory fails ⇒ fatal,
			// orders stays raw.
			name:    "driver build failure is fatal and leaves the child untouched",
			deriver: New(fakeRegistry{}, errDriverFactory{}),
			in: core.DerivedDumpContextInput{
				ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
					tableSpec(1, "app", "users", []string{"user_id"},
						[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
					tableSpec(2, "app", "orders", []string{"order_id"}, nil),
				}},
				TableConfigs: []core.TableConfig{
					tableConfig("app", "users", trCfg("RandomInt", "user_id")),
				},
				DependencyGraphResult: graphFrom(
					fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
				),
			},
			wantErr:            true,
			wantDerived:        map[core.ObjectID][]string{2: nil},
			wantExplicitOrigin: []core.ObjectID{2}, // stays raw/explicit, not flipped
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)
			deriver := tc.deriver
			if deriver == nil {
				deriver = New(fakeRegistry{}, fakeDriverFactory{})
			}

			// Act
			out, err := deriver.Derive(ctx, tc.in)

			// Assert
			if tc.wantErr {
				require.ErrorIs(t, err, core.ErrFatalValidationError)
				assert.True(t, vc.IsFatal())
			} else {
				require.NoError(t, err)
				assert.False(t, vc.IsFatal(), "unexpected fatal warnings: %v", vc.GetWarnings())
				assert.Empty(t, vc.GetWarnings(), "no spurious warnings expected on the happy path")
			}
			for id, want := range tc.wantDerived {
				got := derivedColumns(t, out, id)
				if len(want) == 0 {
					assert.Empty(t, got, "object %d should have no derived transformers", id)
					continue
				}
				assert.Equal(t, want, got, "derived columns on object %d", id)
			}
			for _, id := range tc.wantDerivedOrigin {
				assert.Equal(t, core.ObjectOriginDerived, specByID(out, id).Origin.Kind, "object %d origin", id)
				assert.Equal(t, core.DumpModeTransformed, specByID(out, id).Mode, "object %d mode", id)
			}
			for _, id := range tc.wantExplicitOrigin {
				assert.Equal(t, core.ObjectOriginExplicit, specByID(out, id).Origin.Kind, "object %d origin", id)
			}
		})
	}
}

// TestDerive_DerivedSource checks the provenance stamped on a derived transformer.
//
//	orders(order_id*, user_id) --FK(user_id)--> users(user_id*)[RandomInt, apply_for_references]
//
// Asserts orders.user_id's derived Source.DerivedFrom points back to users.user_id
// via the foreign key.
func TestDerive_DerivedSource(t *testing.T) {
	// Arrange
	in := core.DerivedDumpContextInput{
		ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
			tableSpec(1, "app", "users", []string{"user_id"},
				[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
			tableSpec(2, "app", "orders", []string{"order_id"}, nil),
		}},
		TableConfigs: []core.TableConfig{
			tableConfig("app", "users", trCfg("RandomInt", "user_id")),
		},
		DependencyGraphResult: graphFrom(
			fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
		),
	}
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)
	deriver := New(fakeRegistry{}, fakeDriverFactory{})

	// Act
	out, err := deriver.Derive(ctx, in)

	// Assert
	require.NoError(t, err)
	payload := specByID(out, 2).Payload.(transformercontext.TableDumpContext)
	require.Len(t, payload.TransformerContext, 1)
	derived := payload.TransformerContext[0].(*transformercontext.TransformerContext)

	assert.Equal(t, core.TransformationSourceKindDerived, derived.Source.Kind)
	require.NotNil(t, derived.Source.DerivedFrom)
	assert.Equal(t, core.ObjectLinkKindForeignKey, derived.Source.DerivedFrom.LinkKind)
	assert.Equal(t, core.FieldRefKindColumn, derived.Source.DerivedFrom.Field.Kind)
	assert.Equal(t, "user_id", derived.Source.DerivedFrom.Field.Value)
	// The derivation reference points at the parent (users) object.
	assert.Equal(t, "users", derived.Source.DerivedFrom.Object.NameValues["table"])
}

// TestDerive_NonPrimaryKeyColumnWarns covers apply_for_references on a non-PK column.
//
//	orders(order_id*, user_id) --FK(user_id)--> users(user_id*, email)[RandomInt on EMAIL, apply_for_references]
//
// email is not part of users' PK, so no foreign key references it ⇒ a non-fatal
// warning is emitted and nothing is propagated.
func TestDerive_NonPrimaryKeyColumnWarns(t *testing.T) {
	// Arrange: an eligible transformer opted into apply_for_references, but on a
	// column that is NOT part of the primary key.
	in := core.DerivedDumpContextInput{
		ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
			tableSpec(1, "app", "users", []string{"user_id"},
				[]core.TransformerContexter{rootCtx("RandomInt", "email", true, true, true)}),
			tableSpec(2, "app", "orders", []string{"order_id"}, nil),
		}},
		TableConfigs: []core.TableConfig{
			tableConfig("app", "users", trCfg("RandomInt", "email")),
		},
		DependencyGraphResult: graphFrom(
			fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
		),
	}
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)
	deriver := New(fakeRegistry{}, fakeDriverFactory{})

	// Act
	out, err := deriver.Derive(ctx, in)

	// Assert: non-fatal — the opt-in is surfaced as a warning, the build is not
	// aborted, and nothing is propagated.
	require.NoError(t, err)
	assert.False(t, vc.IsFatal())
	assert.NotEmpty(t, vc.GetWarnings())
	assert.Empty(t, derivedColumns(t, out, 2))
}

// TestDerive_ConflictingRootsWarn covers one column referenced by two parents that
// are anonymized differently.
//
//	users(user_id*)[RandomInt] <--FK(actor_id)-- events(event_id*, actor_id) --FK(actor_id)--> accounts(account_id*)[Hash]
//
// events.actor_id would have to match both a RandomInt- and a Hash-anonymized key
// ⇒ one derived transformer is kept and a conflict warning is emitted (referential
// integrity is unsatisfiable for both).
func TestDerive_ConflictingRootsWarn(t *testing.T) {
	// Arrange: events.actor_id is a foreign key to two parents that anonymize
	// their keys with DIFFERENT transformers, so no single value satisfies both.
	in := core.DerivedDumpContextInput{
		ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
			tableSpec(1, "app", "users", []string{"user_id"},
				[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
			tableSpec(2, "app", "accounts", []string{"account_id"},
				[]core.TransformerContexter{rootCtx("Hash", "account_id", true, true, true)}),
			tableSpec(3, "app", "events", []string{"event_id"}, nil),
		}},
		TableConfigs: []core.TableConfig{
			tableConfig("app", "users", trCfg("RandomInt", "user_id")),
			tableConfig("app", "accounts", trCfg("Hash", "account_id")),
		},
		DependencyGraphResult: graphFrom(
			fkEdge(3, 1, "fk_events_users", []string{"actor_id"}, []string{"user_id"}),
			fkEdge(3, 2, "fk_events_accounts", []string{"actor_id"}, []string{"account_id"}),
		),
	}
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)
	deriver := New(fakeRegistry{}, fakeDriverFactory{})

	// Act
	out, err := deriver.Derive(ctx, in)

	// Assert: non-fatal, but a conflict warning is recorded and the column carries
	// exactly one derived transformer (the second is skipped, not stacked).
	require.NoError(t, err)
	assert.False(t, vc.IsFatal())
	assert.NotEmpty(t, vc.GetWarnings())
	assert.Equal(t, []string{"actor_id"}, derivedColumns(t, out, 3))
}

// TestDerive_DeterministicOrder guards snapshot stability: the order in which
// derived transformers are appended must not depend on Go's randomized map
// iteration, otherwise a transformation's positional snapshot key would change
// between runs and report spurious drift.
//
//	tablec(c1*,c2*) --FK(c1,c2)--> tableb(b1*,b2*) --FK(b1,b2)--> tablea(a1*,a2*)[RandomInt on a1 & a2]
//
// Runs the same input many times; every run must yield the same, column-ordered
// derived sequence on the multi-column children.
func TestDerive_DeterministicOrder(t *testing.T) {
	newInput := func() core.DerivedDumpContextInput {
		return core.DerivedDumpContextInput{
			ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
				tableSpec(1, "app", "tablea", []string{"a1", "a2"},
					[]core.TransformerContexter{
						rootCtx("RandomInt", "a1", true, true, true),
						rootCtx("RandomInt", "a2", true, true, true),
					}),
				tableSpec(2, "app", "tableb", []string{"b1", "b2"}, nil),
				tableSpec(3, "app", "tablec", []string{"c1", "c2"}, nil),
			}},
			TableConfigs: []core.TableConfig{
				tableConfig("app", "tablea", trCfg("RandomInt", "a1"), trCfg("RandomInt", "a2")),
			},
			DependencyGraphResult: graphFrom(
				fkEdge(2, 1, "fk_b_a", []string{"b1", "b2"}, []string{"a1", "a2"}),
				fkEdge(3, 2, "fk_c_b", []string{"c1", "c2"}, []string{"b1", "b2"}),
			),
		}
	}

	// Many iterations make it overwhelmingly likely to observe a reordering if the
	// traversal were map-iteration dependent.
	for range 50 {
		// Arrange
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)

		// Act
		out, err := New(fakeRegistry{}, fakeDriverFactory{}).Derive(ctx, newInput())

		// Assert: stable, column-ordered append sequence on every run.
		require.NoError(t, err)
		assert.Equal(t, []string{"b1", "b2"}, orderedDerivedColumns(out, 2))
		assert.Equal(t, []string{"c1", "c2"}, orderedDerivedColumns(out, 3))
	}
}

// TestDerive_ExplicitDifferentTransformerOnFKWarns covers a FK column the user has
// already taken over with a DIFFERENT transformer than the inherited one.
//
//	orders(order_id*, user_id)[Hash — explicit] --FK(user_id)--> users(user_id*)[RandomInt, apply_for_references]
//
// Propagation must defer to the user's column (no stacked derived transformer) and
// warn, because Hash does not reproduce users' RandomInt-anonymized key.
func TestDerive_ExplicitDifferentTransformerOnFKWarns(t *testing.T) {
	// Arrange
	in := core.DerivedDumpContextInput{
		ExplicitCtx: core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{
			tableSpec(1, "app", "users", []string{"user_id"},
				[]core.TransformerContexter{rootCtx("RandomInt", "user_id", true, true, true)}),
			tableSpec(2, "app", "orders", []string{"order_id"},
				[]core.TransformerContexter{explicitCtx("Hash", "user_id")}),
		}},
		TableConfigs: []core.TableConfig{
			tableConfig("app", "users", trCfg("RandomInt", "user_id")),
		},
		DependencyGraphResult: graphFrom(
			fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"}),
		),
	}
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	// Act
	out, err := New(fakeRegistry{}, fakeDriverFactory{}).Derive(ctx, in)

	// Assert: non-fatal, a warning is raised, nothing is derived onto orders, and
	// the user's explicit Hash is left as the only transformer on the column.
	require.NoError(t, err)
	assert.False(t, vc.IsFatal())
	assert.NotEmpty(t, vc.GetWarnings())
	assert.Empty(t, derivedColumns(t, out, 2))
	payload := specByID(out, 2).Payload.(transformercontext.TableDumpContext)
	require.Len(t, payload.TransformerContext, 1)
	assert.Equal(t, "Hash", payload.TransformerContext[0].Describe())
}

func TestRewriteWhen(t *testing.T) {
	tests := []struct {
		name   string
		when   string
		parent string
		child  string
		want   string
	}{
		{"record namespace", "record.user_id > 0", "user_id", "uid", "record.uid > 0"},
		{"raw_record namespace", "raw_record.user_id != null", "user_id", "uid", "raw_record.uid != null"},
		{"both namespaces", "record.id == 1 && raw_record.id == 2", "id", "ref", "record.ref == 1 && raw_record.ref == 2"},
		{"no match", "record.other > 0", "id", "ref", "record.other > 0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			got := rewriteWhen(tc.when, tc.parent, tc.child)
			// Assert
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestMapColumn(t *testing.T) {
	// Arrange (shared): a composite foreign key with positional alignment.
	fk := core.ForeignKeyLinkPayload{Columns: []string{"c1", "c2"}, RefColumns: []string{"p1", "p2"}}
	tests := []struct {
		name   string
		parent string
		want   string
	}{
		{"first", "p1", "c1"},
		{"second", "p2", "c2"},
		{"missing", "p3", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			got := mapColumn(fk, tc.parent)
			// Assert
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestAffectedPrimaryKeyColumn(t *testing.T) {
	tests := []struct {
		name     string
		affected map[int]string
		pk       []string
		want     string
	}{
		{"pk column", map[int]string{0: "id"}, []string{"id"}, "id"},
		{"non-pk column", map[int]string{3: "email"}, []string{"id"}, ""},
		{"lowest index pk wins", map[int]string{2: "b", 1: "a"}, []string{"a", "b"}, "a"},
		{"empty", nil, []string{"id"}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Act
			got := affectedPrimaryKeyColumn(tc.affected, tc.pk)
			// Assert
			assert.Equal(t, tc.want, got)
		})
	}
}
