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

package plancheck

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// tableIdentity builds a mysql-style table identity (database + table scope).
func tableIdentity(schema, name string) core.EntityIdentity {
	return core.EntityIdentity{
		Kind:      "mysql.table",
		NameParts: []string{"database", "table"},
		NameValues: map[string]string{
			"database": schema,
			"table":    name,
		},
	}
}

// tablePayload builds a TableDumpContext payload carrying the given transformers.
func tablePayload(schema, name string, tcs ...core.TransformerContexter) transformercontext.TableDumpContext {
	return transformercontext.TableDumpContext{
		Table:              &core.Table{Schema: schema, Name: name},
		TransformerContext: tcs,
	}
}

// fkEdge builds a foreign-key ObjectEdge from child to parent.
func fkEdge(child, parent core.ObjectID, constraint string, nullable bool) core.ObjectEdge {
	return core.ObjectEdge{
		From: child,
		To:   parent,
		Link: core.ObjectLink{
			Kind: core.ObjectLinkKindForeignKey,
			Payload: core.ForeignKeyLinkPayload{
				ConstraintName: constraint,
				IsNullable:     nullable,
			},
		},
	}
}

// graphWith builds a dependency graph from named nodes and edges. nodeNames maps
// ObjectID → node name; edges are grouped under their child (From) ObjectID.
func graphWith(nodeNames map[core.ObjectID]string, edges ...core.ObjectEdge) core.DependencyGraphResult {
	nodes := make(map[core.ObjectID]core.ObjectNode, len(nodeNames))
	for id, name := range nodeNames {
		nodes[id] = core.ObjectNode{ID: id, Name: name}
	}
	edgeMap := make(map[core.ObjectID][]core.ObjectEdge)
	for _, e := range edges {
		edgeMap[e.From] = append(edgeMap[e.From], e)
	}
	return core.DependencyGraphResult{
		ObjectGraph: core.ObjectGraph{Nodes: nodes, Edges: edgeMap},
	}
}

func TestCheckRestorationCoversDumpObjects(t *testing.T) {
	tests := []struct {
		name         string
		specs        []core.ObjectDumpSpec
		covered      []core.TaskID
		wantCount    int
		wantSchema   string
		wantTable    string
		wantSeverity core.ValidationSeverity
	}{
		{
			name: "all dumped objects covered by restoration context",
			specs: []core.ObjectDumpSpec{
				{TaskID: 1, Mode: core.DumpModeTransformed, Payload: tablePayload("app", "users")},
				{TaskID: 2, Mode: core.DumpModeRaw, Payload: tablePayload("app", "logs")},
			},
			covered:   []core.TaskID{1, 2},
			wantCount: 0,
		},
		{
			name: "transformed object not covered",
			specs: []core.ObjectDumpSpec{
				{TaskID: 2, Mode: core.DumpModeTransformed, Payload: tablePayload("app", "orders")},
			},
			covered:      []core.TaskID{1},
			wantCount:    1,
			wantSchema:   "app",
			wantTable:    "orders",
			wantSeverity: core.ValidationSeverityError,
		},
		{
			name: "raw object not covered is also flagged",
			specs: []core.ObjectDumpSpec{
				{TaskID: 3, Mode: core.DumpModeRaw, Payload: tablePayload("app", "logs")},
			},
			covered:      []core.TaskID{1},
			wantCount:    1,
			wantSchema:   "app",
			wantTable:    "logs",
			wantSeverity: core.ValidationSeverityError,
		},
		{
			name: "coverage is independent of ordering: cyclic plan with no order still passes",
			specs: []core.ObjectDumpSpec{
				{TaskID: 2, Mode: core.DumpModeTransformed, Payload: tablePayload("app", "orders")},
			},
			// TaskDependencies is populated even when RestorationOrder is empty
			// (cyclic schema), so the object is covered.
			covered:   []core.TaskID{2},
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			deps := make(map[core.TaskID][]core.TaskID, len(tc.covered))
			for _, id := range tc.covered {
				deps[id] = nil
			}
			plan := core.DumpPlan{
				DumpObjectSpecs:    tc.specs,
				RestorationContext: core.RestorationContext{TaskDependencies: deps},
			}

			// Act.
			warnings := checkRestorationCoversDumpObjects(plan)

			// Assert.
			require.Len(t, warnings, tc.wantCount)
			if tc.wantCount > 0 {
				assert.Equal(t, tc.wantSeverity, warnings[0].Severity)
				assert.Equal(t, tc.wantSchema, warnings[0].Meta[core.MetaKeyTableSchema])
				assert.Equal(t, tc.wantTable, warnings[0].Meta[core.MetaKeyTableName])
			}
		})
	}
}

func TestCheckForeignKeyCoverage(t *testing.T) {
	tests := []struct {
		name         string
		dumpedIDs    []core.ObjectID
		graph        core.DependencyGraphResult
		wantCount    int
		wantChild    string
		wantParent   string
		wantNullable bool
	}{
		{
			name:      "child and parent both dumped: no gap",
			dumpedIDs: []core.ObjectID{10, 20},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_users", false),
			),
			wantCount: 0,
		},
		{
			name:      "child dumped, parent excluded: one warning with FK meta",
			dumpedIDs: []core.ObjectID{10},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_users", false),
			),
			wantCount:    1,
			wantChild:    "app.orders",
			wantParent:   "app.users",
			wantNullable: false,
		},
		{
			name:      "nullable FK to excluded parent: warning records nullability",
			dumpedIDs: []core.ObjectID{10},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_users", true),
			),
			wantCount:    1,
			wantChild:    "app.orders",
			wantParent:   "app.users",
			wantNullable: true,
		},
		{
			name:      "parent dumped, child excluded: no gap (child not in dump)",
			dumpedIDs: []core.ObjectID{20},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_users", false),
			),
			wantCount: 0,
		},
		{
			name:      "multiple FK columns to same excluded parent: deduped to one finding",
			dumpedIDs: []core.ObjectID{10},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_created_by", false),
				fkEdge(10, 20, "fk_orders_updated_by", false),
			),
			wantCount:  1,
			wantChild:  "app.orders",
			wantParent: "app.users",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			specs := make([]core.ObjectDumpSpec, 0, len(tc.dumpedIDs))
			for _, id := range tc.dumpedIDs {
				specs = append(specs, core.ObjectDumpSpec{ObjectID: id})
			}
			plan := core.DumpPlan{DumpObjectSpecs: specs}

			// Act.
			warnings := checkForeignKeyCoverage(plan, tc.graph)

			// Assert.
			require.Len(t, warnings, tc.wantCount)
			if tc.wantCount > 0 {
				assert.Equal(t, core.ValidationSeverityWarning, warnings[0].Severity)
				assert.Equal(t, tc.wantChild, warnings[0].Meta[core.MetaKeyTableName])
				assert.Equal(t, tc.wantParent, warnings[0].Meta[metaKeyReferencedObject])
				assert.Equal(t, tc.wantNullable, warnings[0].Meta[metaKeyForeignKeyNullable])
			}
		})
	}
}

func TestValidatePlan(t *testing.T) {
	tests := []struct {
		name        string
		plan        core.DumpPlan
		graph       core.DependencyGraphResult
		wantFatal   bool
		wantWarning int
	}{
		{
			name: "fatal when a dumped object is not covered by the restoration context",
			plan: core.DumpPlan{
				DumpObjectSpecs: []core.ObjectDumpSpec{
					{TaskID: 2, Mode: core.DumpModeTransformed, Payload: tablePayload("app", "orders")},
				},
				// Task 2 is absent from TaskDependencies → uncovered.
				RestorationContext: core.RestorationContext{TaskDependencies: map[core.TaskID][]core.TaskID{1: nil}},
			},
			wantFatal:   true,
			wantWarning: 1,
		},
		{
			name: "not fatal when only a foreign-key coverage gap fires",
			plan: core.DumpPlan{
				DumpObjectSpecs: []core.ObjectDumpSpec{
					{TaskID: 1, ObjectID: 10, Mode: core.DumpModeRaw,
						Identity: tableIdentity("app", "orders"), Payload: tablePayload("app", "orders")},
				},
				RestorationContext: core.RestorationContext{TaskDependencies: map[core.TaskID][]core.TaskID{1: nil}},
			},
			graph: graphWith(
				map[core.ObjectID]string{10: "app.orders", 20: "app.users"},
				fkEdge(10, 20, "fk_orders_users", false),
			),
			wantFatal:   false,
			wantWarning: 1,
		},
		{
			name: "clean plan adds nothing and returns nil",
			plan: core.DumpPlan{
				DumpObjectSpecs: []core.ObjectDumpSpec{
					{TaskID: 1, Mode: core.DumpModeTransformed,
						Identity: tableIdentity("app", "users"),
						Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
						Payload:  tablePayload("app", "users")},
				},
				RestorationContext: core.RestorationContext{TaskDependencies: map[core.TaskID][]core.TaskID{1: nil}},
			},
			wantFatal:   false,
			wantWarning: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Arrange.
			vc := validationcollector.NewCollector()
			ctx := validationcollector.WithCollector(context.Background(), vc)

			// Act.
			err := ValidatePlan(ctx, tc.plan, tc.graph)

			// Assert.
			if tc.wantFatal {
				assert.True(t, errors.Is(err, core.ErrFatalValidationError))
			} else {
				assert.NoError(t, err)
			}
			assert.Len(t, vc.GetWarnings(), tc.wantWarning)
		})
	}
}
