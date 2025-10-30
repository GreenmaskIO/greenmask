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

package condensationgraph

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

func TestNewGraph(t *testing.T) {
	tableA := commonmodels.Table{
		Schema:     "test",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: nil,
	}

	tableB := commonmodels.Table{
		Schema:     "test",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "a",
				Keys:             []string{"a_id"},
				IsNullable:       false,
			},
			{
				ReferencedSchema: "test",
				ReferencedName:   "f",
				Keys:             []string{"f_id"},
				IsNullable:       false,
			},
		},
	}

	tableC := commonmodels.Table{
		Schema:     "test",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "b",
				Keys:             []string{"b_id"},
				IsNullable:       false,
			},
			{
				ReferencedSchema: "test",
				ReferencedName:   "a",
				Keys:             []string{"a_id"},
				IsNullable:       false,
			},
		},
	}

	tableD := commonmodels.Table{
		Schema:     "test",
		Name:       "d",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "d",
				Keys:             []string{"d_id"},
				IsNullable:       false,
			},
		},
	}

	tableF := commonmodels.Table{
		Schema:     "test",
		Name:       "f",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "b",
				Keys:             []string{"b_id"},
				IsNullable:       false,
			},
		},
	}

	tables := []commonmodels.Table{tableA, tableB, tableC, tableD, tableF}

	g, err := tablegraph.NewGraph(tables)
	require.NoError(t, err)

	/*
			There are 3 tables in the Graph: a, b, c

			The Graph should be represented as follows:

					f -          -- (F -> B  has a cycle)
					^  |
					|-<
				a <- b <- c
				|
				 <- c

				d --             -- D has a cycle
				^	|
		        |----
	*/

	cg := NewGraph(g)
	require.NotNil(t, cg)
	require.Len(t, cg.SCC, 4)

	// Validate SCCs
	// SCC 0
	scc0 := cg.SCC[0]
	require.Equal(t, scc0.id, 0)
	require.True(t, scc0.CyclesGraph.HasCycle())
	require.Len(t, scc0.SCCGraph, 1)
	require.Len(t, scc0.vertexes, 1)
	require.Equal(t, scc0.vertexes, map[int]commonmodels.Table{3: tableD})

	// Validate SCCs
	// SCC 1
	scc1 := cg.SCC[1]
	require.Equal(t, scc1.id, 1)
	require.False(t, scc1.CyclesGraph.HasCycle())
	require.Len(t, scc1.SCCGraph, 1)

	edges, ok := scc1.SCCGraph[2]
	require.True(t, ok)
	require.Len(t, edges, 0)

	require.Len(t, scc1.vertexes, 1)
	require.Equal(t, scc1.vertexes, map[int]commonmodels.Table{2: tableC})

	// Validate SCCs
	// SCC 2
	scc2 := cg.SCC[2]
	require.Equal(t, scc2.id, 2)
	require.True(t, scc2.CyclesGraph.HasCycle())
	require.Len(t, scc2.SCCGraph, 2)
	require.Equal(t, scc2.vertexes, map[int]commonmodels.Table{1: tableB, 4: tableF})

	edges, ok = scc2.SCCGraph[1]
	require.True(t, ok)
	require.Len(t, edges, 1)
	require.Equal(t, edges[0].From().Table().Name, "b")
	require.Equal(t, edges[0].To().Table().Name, "f")
	require.Equal(t, edges[0].ID(), 1)

	edges, ok = scc2.SCCGraph[4]
	require.True(t, ok)
	require.Len(t, edges, 1)
	require.Equal(t, edges[0].From().Table().Name, "f")
	require.Equal(t, edges[0].To().Table().Name, "b")
	require.Equal(t, edges[0].ID(), 5)

	// Validate SCCs
	// SCC 3
	scc3 := cg.SCC[3]
	require.Equal(t, scc3.id, 3)
	require.False(t, scc3.CyclesGraph.HasCycle())
	require.Len(t, scc3.SCCGraph, 1)
	require.Equal(t, scc3.vertexes, map[int]commonmodels.Table{0: tableA})

	edges, ok = scc3.SCCGraph[0]
	require.True(t, ok)
	require.Len(t, edges, 0)

	// The Graph
	scc0Edges := cg.Graph[0]
	require.Len(t, scc0Edges, 0)

	scc1Edges := cg.Graph[1]
	require.Len(t, scc1Edges, 2)
	require.Equal(t, scc1Edges[0].from.tableID, 1)
	require.Equal(t, scc1Edges[0].to.tableID, 2)
	require.Equal(t, scc1Edges[1].from.tableID, 1)
	require.Equal(t, scc1Edges[1].to.tableID, 3)

	scc2Edges := cg.Graph[2]
	require.Len(t, scc2Edges, 1)
	require.Equal(t, scc2Edges[0].from.tableID, 2)
	require.Equal(t, scc2Edges[0].to.tableID, 3)

	scc3Edges := cg.Graph[3]
	require.Len(t, scc3Edges, 0)

	// The transposed Graph
	scc0TEdges := cg.TransposedGraph[0]
	require.Len(t, scc0TEdges, 0)

	scc1TEdges := cg.TransposedGraph[1]
	require.Len(t, scc1TEdges, 0)

	scc2TEdges := cg.TransposedGraph[2]
	require.Len(t, scc2TEdges, 1)
	require.Equal(t, scc2TEdges[0].from.tableID, 2)
	require.Equal(t, scc2TEdges[0].to.tableID, 1)

	scc3TEdges := cg.TransposedGraph[3]
	require.Len(t, scc3TEdges, 2)
	require.Equal(t, scc3TEdges[0].from.tableID, 3)
	require.Equal(t, scc3TEdges[0].to.tableID, 2)
	require.Equal(t, scc3TEdges[1].from.tableID, 3)
	require.Equal(t, scc3TEdges[1].to.tableID, 1)
}
