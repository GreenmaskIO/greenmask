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

package subset

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type expectedLink struct {
	from string
	to   string
}

func TestNewSubset(t *testing.T) {
	t.Run("dag with ambiguous tables in join", func(t *testing.T) {
		/*
				There are 5 tables in the graph: a, b, c, d, e

				The graph should be represented as follows:

			       e -> a -> b -> c -> d
						|		 ^
						  -------|
		*/
		tableE := commonmodels.Table{
			ID:         0,
			Schema:     "public",
			Name:       "e",
			PrimaryKey: []string{"id"},
			References: nil,
		}

		tableA := commonmodels.Table{
			ID:         1,
			Schema:     "public",
			Name:       "a",
			PrimaryKey: []string{"id"},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "e",
					Keys:             []string{"e_id"},
					IsNullable:       false,
				},
			},
			SubsetConditions: []string{
				"public.a.id = 1",
			},
		}

		tableB := commonmodels.Table{
			ID:         2,
			Schema:     "public",
			Name:       "b",
			PrimaryKey: []string{"id"},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "a",
					Keys:             []string{"a_id"},
					IsNullable:       false,
				},
			},
		}

		tableC := commonmodels.Table{
			ID:         3,
			Schema:     "public",
			Name:       "c",
			PrimaryKey: []string{"id"},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "b",
					Keys:             []string{"b_id"},
					IsNullable:       false,
				},
				{
					ReferencedSchema: "public",
					ReferencedName:   "a",
					Keys:             []string{"a_id"},
					IsNullable:       false,
				},
			},
			SubsetConditions: []string{
				"public.c.id = 1",
			},
		}

		tableD := commonmodels.Table{
			ID:         4,
			Schema:     "public",
			Name:       "d",
			PrimaryKey: []string{"id"},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "c",
					Keys:             []string{"c_id"},
					IsNullable:       false,
				},
			},
		}

		tables := []commonmodels.Table{tableE, tableA, tableB, tableC, tableD}
		s, err := NewSubset(tables, DialectPostgres)
		require.NoError(t, err)
		require.Len(t, s.subsetGraphs, 5)

		expectedSubGraphs := []map[int][]expectedLink{
			{
				0: {
					{from: "d", to: "c"},
				},
				1: {
					{from: "c", to: "b"},
					{from: "c", to: "a"},
				},
				2: {
					{from: "b", to: "a"},
				},
				3: nil,
			},
			{
				1: {
					{from: "c", to: "b"},
					{from: "c", to: "a"},
				},
				2: {
					{from: "b", to: "a"},
				},
				3: nil,
			},
			{
				2: {
					{from: "b", to: "a"},
				},
				3: nil,
			},
			{
				3: nil,
			},
			{},
		}
		verifySubGraphs(t, expectedSubGraphs, s)
		expectedTablesQueries := []string{
			``,
			`SELECT * FROM "public"."a" WHERE (public.a.id = 1)`,
			`SELECT * FROM "public"."b" INNER JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.a.id = 1)`,
			`SELECT * FROM "public"."c" INNER JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") INNER JOIN "public_a__1" ON ("public"."b"."a_id" = "public_a__1"."id") INNER JOIN "public_a__0" ON ("public"."c"."a_id" = "public_a__0"."id") WHERE (public.c.id = 1)`,
			`SELECT * FROM "public"."d" INNER JOIN "public"."c" ON ("public"."d"."c_id" = "public"."c"."id") INNER JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") INNER JOIN "public_a__1" ON ("public"."b"."a_id" = "public_a__1"."id") INNER JOIN "public_a__0" ON ("public"."c"."a_id" = "public_a__0"."id") WHERE (public.c.id = 1)`,
		}
		require.Equal(t, expectedTablesQueries, s.tablesQueries)
	})

	t.Run("dag with nullable edges", func(t *testing.T) {
		// Create a graph with one edge and two vertices.
		tableA := commonmodels.Table{
			ID:         0,
			Schema:     "public",
			Name:       "a",
			PrimaryKey: []string{"id"},
			SubsetConditions: []string{
				"public.a.id = 1",
			},
		}

		tableB := commonmodels.Table{
			ID:         1,
			Schema:     "public",
			Name:       "b",
			PrimaryKey: []string{"id"},
			SubsetConditions: []string{
				"public.b.id = 1",
			},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "a",
					Keys:             []string{"a_id"},
					IsNullable:       true,
				},
			},
		}

		tableC := commonmodels.Table{
			ID:         2,
			Schema:     "public",
			Name:       "c",
			PrimaryKey: []string{"id"},
			SubsetConditions: []string{
				"public.c.id = 1",
			},
			References: []commonmodels.Reference{
				{
					ReferencedSchema: "public",
					ReferencedName:   "b",
					Keys:             []string{"b_id"},
					IsNullable:       true,
				},
			},
		}

		expectedSubGraphs := []map[int][]expectedLink{
			{
				0: {
					{from: "c", to: "b"},
				},
				1: {
					{from: "b", to: "a"},
				},
				2: nil,
			},
			{
				1: {
					{from: "b", to: "a"},
				},
				2: nil,
			},
			{
				2: nil,
			},
		}
		tables := []commonmodels.Table{tableA, tableB, tableC}
		s, err := NewSubset(tables, DialectPostgres)
		require.NoError(t, err)
		verifySubGraphs(t, expectedSubGraphs, s)
		expectedTablesQueries := []string{
			`SELECT * FROM "public"."a" WHERE (public.a.id = 1)`,
			`SELECT * FROM "public"."b" LEFT JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.b.id = 1) AND (("public"."b"."a_id" IS NULL) OR (public.a.id = 1))`,
			`SELECT * FROM "public"."c" LEFT JOIN "public"."b" ON ("public"."c"."b_id" = "public"."b"."id") LEFT JOIN "public"."a" ON ("public"."b"."a_id" = "public"."a"."id") WHERE (public.c.id = 1) AND (("public"."c"."b_id" IS NULL) OR (public.b.id = 1)) AND (("public"."b"."a_id" IS NULL) OR (public.a.id = 1))`,
		}
		require.Equal(t, expectedTablesQueries, s.tablesQueries)
	})
}

func verifySubGraphs(t *testing.T, expected []map[int][]expectedLink, actual Subset) {
	// Check from actual side
	for i, subGraph := range actual.subsetGraphs {
		if subGraph == nil {
			// If the subGraph is nil, it means that there are no edges in the graph.
			continue
		}
		require.Equal(t, len(subGraph.graph), len(expected[i]))
		for j, edges := range subGraph.graph {
			for edgeI, edge := range edges {
				el := expected[i][j][edgeI]
				actualFrom := edge.OriginalEdge().From().Table().Name
				actualTo := edge.OriginalEdge().To().Table().Name
				require.Equal(t, el.from, actualFrom)
				require.Equal(t, el.to, actualTo)
			}
		}
	}

	// Check from expected side
	for i, subGraph := range expected {
		if len(subGraph) == 0 {
			require.Empty(t, actual.subsetGraphs[i])
		} else {
			require.Equal(t, len(subGraph), len(actual.subsetGraphs[i].graph))
		}
		for j, edges := range subGraph {
			for edgeI, edge := range edges {
				actualFrom := actual.subsetGraphs[i].graph[j][edgeI].OriginalEdge().From().Table().Name
				actualTo := actual.subsetGraphs[i].graph[j][edgeI].OriginalEdge().To().Table().Name
				require.Equal(t, edge.from, actualFrom)
				require.Equal(t, edge.to, actualTo)
			}
		}
	}
}
