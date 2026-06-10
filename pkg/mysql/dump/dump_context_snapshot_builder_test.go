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

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
)

// ignoreSnapshotHashes drops the computed hash/fingerprint fields from
// comparison; their content is deterministic but verbose to hardcode, so they
// are asserted non-empty separately.
var ignoreSnapshotHashes = cmp.Options{
	cmpopts.IgnoreFields(core.DumpContextSnapshot{}, "Key"),
	cmpopts.IgnoreFields(core.ObjectSnapshot{}, "SubsetQueryHash", "AttributesHash"),
	cmpopts.IgnoreFields(core.TransformationSnapshot{}, "ConfigHash", "StaticParametersHash", "DynamicParametersHash", "AffectedColumnsHash", "Fingerprint"),
	cmpopts.IgnoreFields(core.TransformationConditionSnapshot{}, "Fingerprint"),
}

// exprCond is a CondEvaluator that retains its expression, so the snapshot can
// capture a condition without a real compiled program.
type exprCond struct{ expr string }

func (c exprCond) Evaluate(core.Recorder) (bool, error) { return true, nil }
func (c exprCond) Expression() string                   { return c.expr }

// cannedTransformer is a TransformerContexter returning a preset transformation
// snapshot, decoupling the builder test from real parameter initialization
// (covered in the context package tests).
type cannedTransformer struct{ snap core.TransformationSnapshot }

func (cannedTransformer) SetRecordForDynamicParameters(core.Recorder) {}
func (cannedTransformer) EvaluateWhen(core.Recorder) (bool, error)    { return true, nil }
func (cannedTransformer) Init(context.Context) error                  { return nil }
func (cannedTransformer) GetAffectedColumns() map[int]string          { return nil }
func (cannedTransformer) Describe() string                            { return "" }
func (c cannedTransformer) GetSnapshot(int) (core.TransformationSnapshot, error) {
	return c.snap, nil
}

func tableDumpSpec(
	taskID core.TaskID,
	objectID core.ObjectID,
	schema, name string,
	columns []core.Column,
	query string,
	condition core.CondEvaluator,
	transformers []core.TransformerContexter,
) core.ObjectDumpSpec {
	return core.ObjectDumpSpec{
		TaskID:   taskID,
		Kind:     core.ObjectKindMysqlTable,
		ObjectID: objectID,
		Name:     name,
		Identity: mysqlTableIdentity(schema, name),
		Origin:   core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
		Mode:     core.DumpModeRaw,
		Payload: transformercontext.TableDumpContext{
			ColumnKind: core.EntityKindMysqlColumn,
			Table: &core.Table{
				Schema:  schema,
				Name:    name,
				Columns: columns,
			},
			Query:              query,
			Condition:          condition,
			TransformerContext: transformers,
		},
	}
}

// mysqlColAttrs is the expected attribute map for the two-column (id, email)
// tables used in these tests.
func mysqlColAttrs() map[core.StableKey]core.ObjectAttribute {
	return map[core.StableKey]core.ObjectAttribute{
		"mysql.column:id": {
			Identity: core.ColumnAttributeIdentity(core.EntityKindMysqlColumn, "id"),
			Position: 0, Definition: "int",
		},
		"mysql.column:email": {
			Identity: core.ColumnAttributeIdentity(core.EntityKindMysqlColumn, "email"),
			Position: 1, Definition: "varchar",
		},
	}
}

func schemaDumpSpec(taskID core.TaskID, database string) core.SchemaDumpSpec {
	return core.SchemaDumpSpec{
		TaskID:  taskID,
		Kind:    core.SchemaObjectKindMysqlDatabase,
		Payload: core.SchemaDumpContextPayload{Name: database, Section: core.DumpSectionPreData},
	}
}

func TestDumpContextSnapshotBuilder_Build(t *testing.T) {
	cols := []core.Column{
		{Idx: 0, Name: "id", TypeName: "int"},
		{Idx: 1, Name: "email", TypeName: "varchar"},
	}
	transform := core.TransformationSnapshot{
		Key:              "column:email:0:RandomEmail",
		Name:             "RandomEmail",
		Field:            core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: "email"},
		Source:           core.TransformationSource{Kind: core.TransformationSourceKindExplicit},
		StaticParameters: map[string]any{"column": "email"},
	}

	input := core.DumpContext{
		DumpObjectSpecs: []core.ObjectDumpSpec{
			// Transformed table with a condition, in db "app".
			tableDumpSpec(1, 10, "app", "users", cols, "SELECT * FROM users",
				exprCond{expr: "id > 100"},
				[]core.TransformerContexter{cannedTransformer{snap: transform}},
			),
			// Raw table with no condition or transformers in db "app".
			tableDumpSpec(2, 11, "app", "logs", cols, "", nil, nil),
		},
		SchemaDumpSpecs: []core.SchemaDumpSpec{
			schemaDumpSpec(3, "app"),
		},
		Source: mysqlSourceSpec([]string{"app"}, core.DBMSVersion{FullString: "8.0.35"}),
	}

	b := NewDumpContextSnapshotBuilder()
	snap, err := b.Build(context.Background(), input)
	require.NoError(t, err)

	want := core.DumpContextSnapshot{
		SchemaVersion: core.SnapshotSchemaVersionV1,
		Source: core.SourceSnapshot{
			Identity: core.EntityIdentity{
				Kind:       core.EntityKindMysqlServer,
				NameParts:  []string{"databases"},
				NameValues: map[string]string{"databases": "app"},
			},
			DBMSVersion: "8.0.35",
		},
		Objects: map[core.StableKey]core.ObjectSnapshot{
			"mysql.table:app.users": {
				Key: "mysql.table:app.users",
				Identity: core.EntityIdentity{
					Kind:       core.EntityKindMysqlTable,
					NameParts:  []string{"database", "table"},
					NameValues: map[string]string{"database": "app", "table": "users"},
				},
				SubsetQuery: "SELECT * FROM users",
				Attributes:  mysqlColAttrs(),
				Condition: &core.TransformationConditionSnapshot{
					Kind:       core.TransformationConditionKindExpression,
					Expression: "id > 100",
				},
				Transformations: map[core.StableKey]core.TransformationSnapshot{
					"column:email:0:RandomEmail": transform,
				},
				Origin: core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
			},
			// Raw table: structural fields only, no transformations/condition.
			"mysql.table:app.logs": {
				Key: "mysql.table:app.logs",
				Identity: core.EntityIdentity{
					Kind:       core.EntityKindMysqlTable,
					NameParts:  []string{"database", "table"},
					NameValues: map[string]string{"database": "app", "table": "logs"},
				},
				Attributes: mysqlColAttrs(),
				Origin:     core.ObjectOrigin{Kind: core.ObjectOriginExplicit},
			},
		},
	}

	if diff := cmp.Diff(want, snap, ignoreSnapshotHashes); diff != "" {
		t.Errorf("snapshot mismatch (-want +got):\n%s", diff)
	}

	// Computed hashes/fingerprints are ignored above; verify the ones the builder
	// itself computes are populated.
	require.NotEmpty(t, snap.Key)
	users := snap.Objects["mysql.table:app.users"]
	require.NotEmpty(t, users.SubsetQueryHash)
	require.NotEmpty(t, users.AttributesHash)
	require.NotEmpty(t, users.Condition.Fingerprint)
}

func TestDumpContextSnapshotBuilder_Deterministic(t *testing.T) {
	cols := []core.Column{{Idx: 0, Name: "id", TypeName: "int"}}
	transform := core.TransformationSnapshot{
		Key:              "column:id:0:Noise",
		Name:             "Noise",
		Field:            core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: "id"},
		Source:           core.TransformationSource{Kind: core.TransformationSourceKindExplicit},
		StaticParameters: map[string]any{"column": "id", "ratio": "0.1"},
	}
	input := core.DumpContext{
		DumpObjectSpecs: []core.ObjectDumpSpec{
			tableDumpSpec(1, 10, "app", "users", cols, "", nil,
				[]core.TransformerContexter{cannedTransformer{snap: transform}},
			),
		},
		SchemaDumpSpecs: []core.SchemaDumpSpec{schemaDumpSpec(2, "app")},
		Source:          mysqlSourceSpec([]string{"app"}, core.DBMSVersion{FullString: "8.0.35"}),
	}

	b := NewDumpContextSnapshotBuilder()
	first, err := b.Build(context.Background(), input)
	require.NoError(t, err)
	second, err := b.Build(context.Background(), input)
	require.NoError(t, err)

	if diff := cmp.Diff(first, second); diff != "" {
		t.Errorf("snapshot must be deterministic (-first +second):\n%s", diff)
	}
}
