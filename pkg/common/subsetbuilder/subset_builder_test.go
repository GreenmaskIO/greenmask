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

package subsetbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/graphbuilder"
)

// makeIntrospection builds an IntrospectionResult with the given tables as
// ObjectKindTable payloads. objectIDs must be the same length as tables.
func makeIntrospection(objectIDs []core.ObjectID, tables []core.Table) core.IntrospectionResult {
	objects := make([]core.Object, len(tables))
	for i, tbl := range tables {
		objects[i] = core.Object{
			ID:      objectIDs[i],
			Kind:    core.ObjectKindTable,
			Name:    tbl.Schema + "." + tbl.Name,
			Payload: tbl,
		}
	}
	return core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: objects,
		},
	}
}

// makeDependencyGraph computes the DependencyGraphResult from an introspection
// using the shared GraphBuilder.  Tests use this to populate the DependencyGraph
// field of SubsetBuilderInput because the new implementation consumes the
// already-computed graph rather than rebuilding it internally.
func makeDependencyGraph(t *testing.T, introspection core.IntrospectionResult) core.DependencyGraphResult {
	t.Helper()
	dg, err := graphbuilder.New(core.ObjectKindTable).BuildGraph(context.Background(), introspection)
	require.NoError(t, err)
	return dg
}

// tableConfig is a shorthand for building a TableConfig with subset conditions.
func tableConfig(schema, name string, conds ...string) core.TableConfig {
	return core.TableConfig{Schema: schema, Name: name, SubsetConds: conds}
}

func TestBuildSubset_DAGWithAmbiguousTables(t *testing.T) {
	/*
			There are 5 tables in the graph: e, a, b, c, d

			The graph is represented as follows:

		       e -> a -> b -> c -> d
					|		 ^
					  -------|

		Only tables a and c have user-defined subset conditions.
		Tables e, b, d have no conditions and are not present in the result map
		unless they are reachable from a subset-conditioned table.

		This mirrors the "dag with ambiguous tables in join" test from subset_test.go.
	*/
	tableE := core.Table{
		Schema:     "public",
		Name:       "e",
		PrimaryKey: []string{"id"},
	}
	tableA := core.Table{
		Schema:     "public",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "e", Keys: []string{"e_id"}},
		},
	}
	tableB := core.Table{
		Schema:     "public",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
		},
	}
	tableC := core.Table{
		Schema:     "public",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "b", Keys: []string{"b_id"}},
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
		},
	}
	tableD := core.Table{
		Schema:     "public",
		Name:       "d",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "c", Keys: []string{"c_id"}},
		},
	}

	// ObjectIDs are intentionally non-sequential to exercise the ObjectID→position mapping.
	objectIDs := []core.ObjectID{10, 11, 12, 13, 14}
	tables := []core.Table{tableE, tableA, tableB, tableC, tableD}
	introspection := makeIntrospection(objectIDs, tables)

	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
		tableConfig("public", "c", "public.c.id = 1"),
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)

	// Table e has no subset conditions and is not downstream of any — not in map.
	require.NotContains(t, result.SubsetMap, core.ObjectID(10))

	require.Equal(t,
		"SELECT `public`.`a`.* FROM `public`.`a` WHERE (public.a.id = 1)",
		result.SubsetMap[11],
	)
	require.Equal(t,
		"SELECT `public`.`b`.* FROM `public`.`b` INNER JOIN `public`.`a` ON (`public`.`b`.`a_id` = `public`.`a`.`id`) WHERE (public.a.id = 1)",
		result.SubsetMap[12],
	)
	require.Equal(t,
		"SELECT `public`.`c`.* FROM `public`.`c` INNER JOIN `public`.`b` ON (`public`.`c`.`b_id` = `public`.`b`.`id`) INNER JOIN `public_a__1` ON (`public`.`b`.`a_id` = `public_a__1`.`id`) INNER JOIN `public_a__0` ON (`public`.`c`.`a_id` = `public_a__0`.`id`) WHERE (public.c.id = 1) AND (public_a__1.id = 1) AND (public_a__0.id = 1)",
		result.SubsetMap[13],
	)
	require.Equal(t,
		"SELECT `public`.`d`.* FROM `public`.`d` INNER JOIN `public`.`c` ON (`public`.`d`.`c_id` = `public`.`c`.`id`) INNER JOIN `public`.`b` ON (`public`.`c`.`b_id` = `public`.`b`.`id`) INNER JOIN `public_a__1` ON (`public`.`b`.`a_id` = `public_a__1`.`id`) INNER JOIN `public_a__0` ON (`public`.`c`.`a_id` = `public_a__0`.`id`) WHERE (public.c.id = 1) AND (public_a__1.id = 1) AND (public_a__0.id = 1)",
		result.SubsetMap[14],
	)
	require.Len(t, result.SubsetMap, 4)
}

func TestBuildSubset_DAGWithNullableEdges(t *testing.T) {
	/*
		Three tables: a <- b <- c
		All three have subset conditions; b->a and c->b FKs are nullable.

		This mirrors the "dag with nullable edges" test from subset_test.go.
	*/
	tableA := core.Table{
		Schema:     "public",
		Name:       "a",
		PrimaryKey: []string{"id"},
	}
	tableB := core.Table{
		Schema:     "public",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}, IsNullable: true},
		},
	}
	tableC := core.Table{
		Schema:     "public",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "b", Keys: []string{"b_id"}, IsNullable: true},
		},
	}

	objectIDs := []core.ObjectID{20, 21, 22}
	tables := []core.Table{tableA, tableB, tableC}
	introspection := makeIntrospection(objectIDs, tables)

	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
		tableConfig("public", "b", "public.b.id = 1"),
		tableConfig("public", "c", "public.c.id = 1"),
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)

	require.Equal(t,
		"SELECT `public`.`a`.* FROM `public`.`a` WHERE (public.a.id = 1)",
		result.SubsetMap[20],
	)
	require.Equal(t,
		"SELECT `public`.`b`.* FROM `public`.`b` LEFT JOIN `public`.`a` ON (`public`.`b`.`a_id` = `public`.`a`.`id`) WHERE (public.b.id = 1) AND ((`public`.`b`.`a_id` IS NULL) OR (public.a.id = 1))",
		result.SubsetMap[21],
	)
	require.Equal(t,
		"SELECT `public`.`c`.* FROM `public`.`c` LEFT JOIN `public`.`b` ON (`public`.`c`.`b_id` = `public`.`b`.`id`) LEFT JOIN `public`.`a` ON (`public`.`b`.`a_id` = `public`.`a`.`id`) WHERE (public.c.id = 1) AND ((`public`.`c`.`b_id` IS NULL) OR (public.b.id = 1)) AND ((`public`.`b`.`a_id` IS NULL) OR (public.a.id = 1))",
		result.SubsetMap[22],
	)
	require.Len(t, result.SubsetMap, 3)
}

func TestBuildSubset_PostgreSQL_DAGWithAmbiguousTables(t *testing.T) {
	// PostgreSQL counterpart of TestBuildSubset_DAGWithAmbiguousTables.
	// Verifies double-quote identifier escaping produced by DialectPostgres.
	tableE := core.Table{Schema: "public", Name: "e", PrimaryKey: []string{"id"}}
	tableA := core.Table{
		Schema:     "public",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "e", Keys: []string{"e_id"}},
		},
	}
	tableB := core.Table{
		Schema:     "public",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
		},
	}
	tableC := core.Table{
		Schema:     "public",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "b", Keys: []string{"b_id"}},
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
		},
	}
	tableD := core.Table{
		Schema:     "public",
		Name:       "d",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "c", Keys: []string{"c_id"}},
		},
	}

	objectIDs := []core.ObjectID{10, 11, 12, 13, 14}
	tables := []core.Table{tableE, tableA, tableB, tableC, tableD}
	introspection := makeIntrospection(objectIDs, tables)

	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
		tableConfig("public", "c", "public.c.id = 1"),
	}

	b := New(DialectPostgres, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)

	require.NotContains(t, result.SubsetMap, core.ObjectID(10))

	require.Equal(t,
		`SELECT "public"."a".* FROM "public"."a" WHERE (public.a.id = 1)`,
		result.SubsetMap[11],
	)
	require.Equal(t,
		`SELECT "public"."b".* FROM "public"."b" INNER JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.a.id = 1)`,
		result.SubsetMap[12],
	)
	require.Equal(t,
		`SELECT "public"."c".* FROM "public"."c" INNER JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") INNER JOIN "public_a__1" ON ("public"."b"."a_id" = "public_a__1"."id") INNER JOIN "public_a__0" ON ("public"."c"."a_id" = "public_a__0"."id") WHERE (public.c.id = 1) AND (public_a__1.id = 1) AND (public_a__0.id = 1)`,
		result.SubsetMap[13],
	)
	require.Equal(t,
		`SELECT "public"."d".* FROM "public"."d" INNER JOIN "public"."c" ON ("public"."d"."c_id" = "public"."c"."id") INNER JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") INNER JOIN "public_a__1" ON ("public"."b"."a_id" = "public_a__1"."id") INNER JOIN "public_a__0" ON ("public"."c"."a_id" = "public_a__0"."id") WHERE (public.c.id = 1) AND (public_a__1.id = 1) AND (public_a__0.id = 1)`,
		result.SubsetMap[14],
	)
	require.Len(t, result.SubsetMap, 4)
}

func TestBuildSubset_PostgreSQL_DAGWithNullableEdges(t *testing.T) {
	// PostgreSQL counterpart of TestBuildSubset_DAGWithNullableEdges.
	tableA := core.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
	tableB := core.Table{
		Schema:     "public",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}, IsNullable: true},
		},
	}
	tableC := core.Table{
		Schema:     "public",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "b", Keys: []string{"b_id"}, IsNullable: true},
		},
	}

	objectIDs := []core.ObjectID{20, 21, 22}
	tables := []core.Table{tableA, tableB, tableC}
	introspection := makeIntrospection(objectIDs, tables)

	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
		tableConfig("public", "b", "public.b.id = 1"),
		tableConfig("public", "c", "public.c.id = 1"),
	}

	b := New(DialectPostgres, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)

	require.Equal(t,
		`SELECT "public"."a".* FROM "public"."a" WHERE (public.a.id = 1)`,
		result.SubsetMap[20],
	)
	require.Equal(t,
		`SELECT "public"."b".* FROM "public"."b" LEFT JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.b.id = 1) AND (("public"."b"."a_id" IS NULL) OR (public.a.id = 1))`,
		result.SubsetMap[21],
	)
	require.Equal(t,
		`SELECT "public"."c".* FROM "public"."c" LEFT JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") LEFT JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.c.id = 1) AND (("public"."c"."b_id" IS NULL) OR (public.b.id = 1)) AND (("public"."b"."a_id" IS NULL) OR (public.a.id = 1))`,
		result.SubsetMap[22],
	)
	require.Len(t, result.SubsetMap, 3)
}

func TestBuildSubset_NoSubsetConditions(t *testing.T) {
	// Tables without any subset conditions produce an empty SubsetMap.
	tableA := core.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
	tableB := core.Table{
		Schema:     "public",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []core.Reference{
			{ReferencedSchema: "public", ReferencedName: "a", Keys: []string{"a_id"}},
		},
	}
	introspection := makeIntrospection(
		[]core.ObjectID{30, 31},
		[]core.Table{tableA, tableB},
	)

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
	})
	require.NoError(t, err)
	require.Empty(t, result.SubsetMap)
}

func TestBuildSubset_EmptyIntrospection(t *testing.T) {
	// No tables at all — should return an empty result without error.
	introspection := core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{},
	}
	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
	})
	require.NoError(t, err)
	require.Empty(t, result.SubsetMap)
}

func TestBuildSubset_PointerTablePayload(t *testing.T) {
	// Object payload is *models.Table — the *Table branch of tableFromPayload.
	tbl := core.Table{
		Schema:     "public",
		Name:       "a",
		PrimaryKey: []string{"id"},
	}
	introspection := core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				{ID: 40, Kind: core.ObjectKindTable, Name: "public.a", Payload: &tbl},
			},
		},
	}
	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)
	require.Equal(t,
		"SELECT `public`.`a`.* FROM `public`.`a` WHERE (public.a.id = 1)",
		result.SubsetMap[40],
	)
}

func TestBuildSubset_ToCommonTablePayload(t *testing.T) {
	// Object payload implements ToCommonTable() — the interface branch of tableFromPayload.
	// tableWithToCommonTable is defined at package level (below) since methods
	// cannot be declared on local types inside a function body.
	payload := &tableWithToCommonTable{tbl: core.Table{
		Schema:     "public",
		Name:       "a",
		PrimaryKey: []string{"id"},
	}}

	introspection := core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				{ID: 50, Kind: core.ObjectKindTable, Name: "public.a", Payload: payload},
			},
		},
	}
	cfgs := []core.TableConfig{
		tableConfig("public", "a", "public.a.id = 1"),
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)
	require.Equal(t,
		"SELECT `public`.`a`.* FROM `public`.`a` WHERE (public.a.id = 1)",
		result.SubsetMap[50],
	)
}

func TestBuildSubset_InvalidPayload(t *testing.T) {
	// Object payload is an unsupported type.
	// In the new implementation buildSubsetCondsMap silently skips objects with
	// unparseable payloads — the error would have been caught earlier by GraphBuilder.
	// The result is an empty SubsetMap (no matching conditions could be resolved).
	introspection := core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				{ID: 60, Kind: core.ObjectKindTable, Name: "public.a", Payload: "not-a-table"},
			},
		},
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: core.DependencyGraphResult{},
	})
	require.NoError(t, err)
	require.Empty(t, result.SubsetMap)
}

func TestBuildSubset_NilPointerPayload(t *testing.T) {
	// Object payload is a nil *models.Table pointer.
	// Same as InvalidPayload: silently skipped in buildSubsetCondsMap.
	var nilTable *core.Table
	introspection := core.IntrospectionResult{
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				{ID: 70, Kind: core.ObjectKindTable, Name: "public.a", Payload: nilTable},
			},
		},
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: core.DependencyGraphResult{},
	})
	require.NoError(t, err)
	require.Empty(t, result.SubsetMap)
}

func TestBuildSubset_SubsetCondOnlyForMatchingTable(t *testing.T) {
	// A TableConfig whose schema+name does not match any introspected table is silently ignored.
	tableA := core.Table{Schema: "public", Name: "a", PrimaryKey: []string{"id"}}
	introspection := makeIntrospection([]core.ObjectID{80}, []core.Table{tableA})

	cfgs := []core.TableConfig{
		tableConfig("public", "nonexistent", "public.nonexistent.id = 1"),
	}

	b := New(DialectMySQL, core.ObjectKindTable)
	result, err := b.BuildSubset(context.Background(), core.SubsetBuilderInput{
		Introspection:   introspection,
		DependencyGraph: makeDependencyGraph(t, introspection),
		TableConfigs:    cfgs,
	})
	require.NoError(t, err)
	require.Empty(t, result.SubsetMap)
}

// tableWithToCommonTable is a package-level type used by TestBuildSubset_ToCommonTablePayload
// to exercise the ToCommonTable() interface branch of tableFromPayload.
type tableWithToCommonTable struct {
	tbl core.Table
}

func (w *tableWithToCommonTable) ToCommonTable() core.Table {
	return w.tbl
}
