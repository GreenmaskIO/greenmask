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
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/tablebuilder"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/mysql/dbmsdriver"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

// intColumn builds a MySQL INT column definition usable by real transformer init.
func intColumn(idx int, name string) core.Column {
	return core.Column{
		Idx:  idx,
		Name: name,
		Type: core.Type{
			Name:  dbmsdriver.TypeInt,
			ID:    dbmsdriver.TypeIDInt,
			Class: core.TypeClassInt,
			Size:  4,
		},
	}
}

// randomIntConfig builds a deterministic RandomInt transformer config on a column.
func randomIntConfig(column string, applyForReferences bool) core.TransformerConfig {
	return core.TransformerConfig{
		Name:               "RandomInt",
		ApplyForReferences: applyForReferences,
		StaticParams: map[string]core.ParamsValue{
			"column": core.ParamsValue(column),
			"engine": core.ParamsValue("deterministic"),
		},
	}
}

// buildTransformedSpec initialises real transformers for a table and returns a
// transformed ObjectDumpSpec, mirroring what the explicit builder produces.
func buildTransformedSpec(
	t *testing.T, ctx context.Context,
	id core.ObjectID, schema, name string, pk []string,
	columns []core.Column, configs []core.TransformerConfig,
) core.ObjectDumpSpec {
	t.Helper()
	table := core.Table{Schema: schema, Name: name, Columns: columns, PrimaryKey: pk}
	driver, err := mysqlDriverFactory{}.NewTableDriver(ctx, table, nil)
	require.NoError(t, err)
	tctxs, err := tablebuilder.InitTableTransformers(ctx, driver, configs, registry.DefaultTransformerRegistry.Core())
	require.NoError(t, err)
	return core.ObjectDumpSpec{
		ObjectID: id,
		Kind:     kinds.ObjectKindTable,
		Name:     name,
		Identity: mysqlTableIdentity(schema, name),
		Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
		Mode:     core.DumpModeTransformed,
		Payload: transformercontext.TableDumpContext{
			ColumnKind:         core.EntityKindMysqlColumn,
			Table:              &table,
			TransformerContext: tctxs,
			TableDriver:        driver,
		},
	}
}

// buildRawSpec returns a raw ObjectDumpSpec (no transformers, no driver).
func buildRawSpec(
	id core.ObjectID, schema, name string, pk []string, columns []core.Column,
) core.ObjectDumpSpec {
	table := core.Table{Schema: schema, Name: name, Columns: columns, PrimaryKey: pk}
	return core.ObjectDumpSpec{
		ObjectID: id,
		Kind:     kinds.ObjectKindTable,
		Name:     name,
		Identity: mysqlTableIdentity(schema, name),
		Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
		Mode:     core.DumpModeRaw,
		Payload: transformercontext.TableDumpContext{
			ColumnKind: core.EntityKindMysqlColumn,
			Table:      &table,
		},
	}
}

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

// findTransformation returns the transformation snapshot on an object whose field
// value matches column.
func findTransformation(
	obj core.ObjectSnapshot, column string,
) (core.TransformationSnapshot, bool) {
	for _, ts := range obj.Transformations {
		if ts.Field.Value == column {
			return ts, true
		}
	}
	return core.TransformationSnapshot{}, false
}

// TestDerivedDumpContextBuilder_Snapshot exercises the rewritten MySQL derived
// builder end-to-end: it propagates a deterministic primary-key transformer onto
// referencing foreign-key columns, then builds the dump snapshot and asserts the
// derived markers surface correctly.
//
// Layout:
//   - users(user_id PK, RandomInt deterministic apply_for_references)
//   - orders(order_id PK, user_id FK -> users.user_id)  [raw]
//   - logs(log_id PK RandomInt explicit, user_id FK -> users.user_id) [transformed]
func TestDerivedDumpContextBuilder_Snapshot(t *testing.T) {
	vc := validationcollector.NewCollector()
	ctx := validationcollector.WithCollector(context.Background(), vc)

	users := buildTransformedSpec(t, ctx, 1, "app", "users", []string{"user_id"},
		[]core.Column{intColumn(0, "user_id")},
		[]core.TransformerConfig{randomIntConfig("user_id", true)},
	)
	orders := buildRawSpec(2, "app", "orders", []string{"order_id"},
		[]core.Column{intColumn(0, "order_id"), intColumn(1, "user_id")},
	)
	logs := buildTransformedSpec(t, ctx, 3, "app", "logs", []string{"log_id"},
		[]core.Column{intColumn(0, "log_id"), intColumn(1, "user_id")},
		[]core.TransformerConfig{randomIntConfig("log_id", false)},
	)

	explicitCtx := core.DumpContext{
		DumpObjectSpecs: []core.ObjectDumpSpec{users, orders, logs},
		Source:          mysqlSourceSpec([]string{"app"}, core.DBMSVersion{FullString: "8.0.35"}),
	}

	in := core.DerivedDumpContextInput{
		ExplicitCtx: explicitCtx,
		TableConfigs: []core.TableConfig{
			{Schema: "app", Name: "users", Transformers: []core.TransformerConfig{randomIntConfig("user_id", true)}},
			{Schema: "app", Name: "logs", Transformers: []core.TransformerConfig{randomIntConfig("log_id", false)}},
		},
		DependencyGraphResult: core.DependencyGraphResult{
			ObjectGraph: core.ObjectGraph{
				Edges: map[core.ObjectID][]core.ObjectEdge{
					2: {fkEdge(2, 1, "fk_orders_users", []string{"user_id"}, []string{"user_id"})},
					3: {fkEdge(3, 1, "fk_logs_users", []string{"user_id"}, []string{"user_id"})},
				},
			},
		},
	}

	builder := NewDerivedDumpContextBuilder(registry.DefaultTransformerRegistry.Core())
	finalCtx, err := builder.BuildDumpContext(ctx, in)
	require.NoError(t, err)
	require.False(t, vc.IsFatal(), "unexpected fatal warnings: %v", vc.GetWarnings())
	require.Empty(t, vc.GetWarnings(), "derivation should not emit spurious warnings")

	snap, err := NewDumpContextSnapshotBuilder().Build(ctx, finalCtx)
	require.NoError(t, err) // no duplicate-key error despite explicit+derived on logs

	// orders: raw child flipped to derived origin, single derived transformation.
	ordersSnap := snap.Objects["mysql.table:app.orders"]
	require.Equal(t, core.ObjectOriginDerived, ordersSnap.Origin.Kind)
	require.Len(t, ordersSnap.Transformations, 1)
	ordersTr, ok := findTransformation(ordersSnap, "user_id")
	require.True(t, ok)
	require.Equal(t, core.TransformationSourceKindDerived, ordersTr.Source.Kind)
	require.NotNil(t, ordersTr.Source.DerivedFrom)
	require.Equal(t, "user_id", ordersTr.Source.DerivedFrom.Field.Value)
	require.Equal(t, core.ObjectLinkKindForeignKey, ordersTr.Source.DerivedFrom.LinkKind)
	require.Equal(t, mysqlTableIdentity("app", "users"), ordersTr.Source.DerivedFrom.Object)

	// logs: already-explicit object keeps explicit origin; explicit and derived
	// transformations coexist on different columns without a key collision.
	logsSnap := snap.Objects["mysql.table:app.logs"]
	require.Equal(t, core.ObjectOriginExplicit, logsSnap.Origin.Kind)
	require.Len(t, logsSnap.Transformations, 2)
	logExplicit, ok := findTransformation(logsSnap, "log_id")
	require.True(t, ok)
	require.Equal(t, core.TransformationSourceKindExplicit, logExplicit.Source.Kind)
	logDerived, ok := findTransformation(logsSnap, "user_id")
	require.True(t, ok)
	require.Equal(t, core.TransformationSourceKindDerived, logDerived.Source.Kind)

	// users: the root transformer remains explicit.
	usersSnap := snap.Objects["mysql.table:app.users"]
	require.Equal(t, core.ObjectOriginExplicit, usersSnap.Origin.Kind)
	usersTr, ok := findTransformation(usersSnap, "user_id")
	require.True(t, ok)
	require.Equal(t, core.TransformationSourceKindExplicit, usersTr.Source.Kind)
}
