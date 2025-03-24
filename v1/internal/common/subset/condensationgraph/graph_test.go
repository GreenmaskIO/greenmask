package condensationgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewGraph(t *testing.T) {
	tableA := common.Table{
		Schema:     "test",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: nil,
	}

	tableB := common.Table{
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

	tableC := common.Table{
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

	tableD := common.Table{
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

	tableF := common.Table{
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

	tables := []common.Table{tableA, tableB, tableC, tableD, tableF}

	g, err := tablegraph.NewGraph(tables)
	require.NoError(t, err)

	/*
			There are 3 tables in the graph: a, b, c

			The graph should be represented as follows:

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
	require.Len(t, cg.scc, 4)

	// Validate SCCs
	// SCC 0
	scc0 := cg.scc[0]
	require.Equal(t, scc0.id, 0)
	require.True(t, scc0.cyclesGraph.HasCycle())
	require.Len(t, scc0.sccGraph, 1)
	require.Len(t, scc0.vertexes, 1)
	require.Equal(t, scc0.vertexes, map[int]common.Table{3: tableD})

	// Validate SCCs
	// SCC 1
	scc1 := cg.scc[1]
	require.Equal(t, scc1.id, 1)
	require.False(t, scc1.cyclesGraph.HasCycle())
	require.Len(t, scc1.sccGraph, 1)

	edges, ok := scc1.sccGraph[2]
	require.True(t, ok)
	require.Len(t, edges, 0)

	require.Len(t, scc1.vertexes, 1)
	require.Equal(t, scc1.vertexes, map[int]common.Table{2: tableC})

	// Validate SCCs
	// SCC 2
	scc2 := cg.scc[2]
	require.Equal(t, scc2.id, 2)
	require.True(t, scc2.cyclesGraph.HasCycle())
	require.Len(t, scc2.sccGraph, 2)
	require.Equal(t, scc2.vertexes, map[int]common.Table{1: tableB, 4: tableF})

	edges, ok = scc2.sccGraph[1]
	require.True(t, ok)
	require.Len(t, edges, 1)
	require.Equal(t, edges[0].From().Table().Name, "b")
	require.Equal(t, edges[0].To().Table().Name, "f")
	require.Equal(t, edges[0].ID(), 1)

	edges, ok = scc2.sccGraph[4]
	require.True(t, ok)
	require.Len(t, edges, 1)
	require.Equal(t, edges[0].From().Table().Name, "f")
	require.Equal(t, edges[0].To().Table().Name, "b")
	require.Equal(t, edges[0].ID(), 5)

	// Validate SCCs
	// SCC 3
	scc3 := cg.scc[3]
	require.Equal(t, scc3.id, 3)
	require.False(t, scc3.cyclesGraph.HasCycle())
	require.Len(t, scc3.sccGraph, 1)
	require.Equal(t, scc3.vertexes, map[int]common.Table{0: tableA})

	edges, ok = scc3.sccGraph[0]
	require.True(t, ok)
	require.Len(t, edges, 0)

	// The graph
	scc0Edges := cg.graph[0]
	require.Len(t, scc0Edges, 0)

	scc1Edges := cg.graph[1]
	require.Len(t, scc1Edges, 2)
	require.Equal(t, scc1Edges[0].from.sscID, 1)
	require.Equal(t, scc1Edges[0].to.sscID, 2)
	require.Equal(t, scc1Edges[1].from.sscID, 1)
	require.Equal(t, scc1Edges[1].to.sscID, 3)

	scc2Edges := cg.graph[2]
	require.Len(t, scc2Edges, 1)
	require.Equal(t, scc2Edges[0].from.sscID, 2)
	require.Equal(t, scc2Edges[0].to.sscID, 3)

	scc3Edges := cg.graph[3]
	require.Len(t, scc3Edges, 0)

	// The transposed graph
	scc0TEdges := cg.transposedGraph[0]
	require.Len(t, scc0TEdges, 0)

	scc1TEdges := cg.transposedGraph[1]
	require.Len(t, scc1TEdges, 0)

	scc2TEdges := cg.transposedGraph[2]
	require.Len(t, scc2TEdges, 1)
	require.Equal(t, scc2TEdges[0].from.sscID, 2)
	require.Equal(t, scc2TEdges[0].to.sscID, 1)

	scc3TEdges := cg.transposedGraph[3]
	require.Len(t, scc3TEdges, 2)
	require.Equal(t, scc3TEdges[0].from.sscID, 3)
	require.Equal(t, scc3TEdges[0].to.sscID, 2)
	require.Equal(t, scc3TEdges[1].from.sscID, 3)
	require.Equal(t, scc3TEdges[1].to.sscID, 1)
}
