package subset

import (
	"fmt"
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// Subset - represents a subset engine for the tables.
//
// It generates the subset queries for the tables.
type Subset struct {
	tables            []common.Table
	tableGraph        tablegraph.Graph
	condensationGraph condensationgraph.Graph
}

// NewSubset - creates a new Subset instance.
func NewSubset(tables []common.Table) (Subset, error) {
	tableGraph, err := tablegraph.NewGraph(tables)
	if err != nil {
		return Subset{}, fmt.Errorf("create table graph: %w", err)
	}
	condensationGraph := condensationgraph.NewGraph(tableGraph)
	return Subset{
		tables:            tables,
		tableGraph:        tableGraph,
		condensationGraph: condensationGraph,
	}, nil
}

// Generate - generates the subset queries for the tables.
//
// Each key of the map represents the tableID that is the index of the table in the tables slice.
// The value is the subset query for the table.
func (s *Subset) Generate() (map[int]string, error) {
	panic("implement me")
}

// findSubsetVertexes - finds the subset vertexes in the graph
func (s *Subset) findSubsetVertexes() {
	for v := range g.condensedGraph {
		path := NewPath(v)
		var from, fullFrom []*CondensedEdge
		if len(g.scc[v].getSubsetConds()) > 0 || g.scc[v].hasPolymorphicExpressions() {
			path.AddVertex(v)
		}
		g.subsetDfs(path, v, &fullFrom, &from, rootScopeId)

		if path.Len() > 0 {
			g.paths[v] = path
		}
	}
}

func (s *Subset) subsetDfs(path *Path, v int, fullFrom, from *[]*CondensedEdge, scopeId int) {
	for _, to := range g.condensedGraph[v] {
		*fullFrom = append(*fullFrom, to)
		*from = append(*from, to)
		currentScopeId := scopeId
		if len(g.scc[to.to.idx].getSubsetConds()) > 0 || (*fullFrom)[len(*fullFrom)-1].hasPolymorphicExpressions() {
			for _, e := range *from {
				currentScopeId = path.AddEdge(e, currentScopeId)
			}
			*from = (*from)[:0]
		}
		g.subsetDfs(path, to.to.idx, fullFrom, from, currentScopeId)
		*fullFrom = (*fullFrom)[:len(*fullFrom)-1]
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
}
