package subset

import (
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// The tasks to resolve
//
// FIND AFFECTED VERTEXES AND BUILD PATH
// 1. Build all graphs
// 2. Use Condensation graph for building a query path since it's a DAG
// 3. Store each query path to the list of queries
//
// Plan query
// There are at least three possible cases to plan a query
//  1. Query do not have any JOIN's (edges)
//  2. Query with simple JOIN's
//  3. One of the vertexes has SCC with cycle
//

// I believe we can store the required sub-graph in map[int][]condensationgraph.Edge.
// If there is a table that has only one vertex without JOIN's then we will use only
// the vertex ID for a query

// Subset - represents a subset engine for the tables.
//
// It generates the subset queries for the tables.
type Subset struct {
	tables            []common.Table
	tableGraph        tablegraph.Graph
	condensationGraph condensationgraph.Graph
	// sccsGraph - is a list of graphs assigned to the specific SCC.
	// The index of slice represents SCC Idx, meaning you can find SCC
	// in SCC list by Idx.
	sccsGraph []map[int][]condensationgraph.Edge
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
		sccsGraph:         make([]map[int][]condensationgraph.Edge, len(condensationGraph.SCC)),
	}, nil
}

// searchTablesGraph - find sub-graphs for tables that has at least one subset condition in the path.
//
// Each key of the map represents the tableID that is the index of the table in the tables slice.
// The value is the subset query for the table.
func (s *Subset) searchTablesGraph() {
	// We start the DFS from each vertex.
	// When Subset condition is found we have to add all edges start from the beginning
	// (or the previous vertex with subset). Those edges must be added as a graph.
	//
	//  For example
	// 		The graph 1 -> 2 -> 3 -> 4 -> 5 -> 6
	//		List of vertexes with subset: 3, 5
	//  	Then
	//			Once subset 3 is met - add the edges with vertexes 1 -> 2 -> 3 into graph
	//				The result: 1 -> 2 -> 3
	//			Once subset 5 is met - add the edges with vertexes 4 -> 5 into graph
	//				The result: 1 -> 2 -> 3 -> 4 -> 5
	//			The vertex 6 will be skipped because it does not affect the previous, because it does not has
	//			subset conditions.
	//
	// The resulting Sub-Graph will be used for query planning later.
	for v := range s.condensationGraph.Graph {
		var (
			from *[]condensationgraph.Edge
			// sscsSubGraph - the graph that contains all the vertexes (SCC) that are affected
			// by subset conditions. This will be used for query planning.
			sscsSubGraph = make(map[int][]condensationgraph.Edge)
		)
		if s.condensationGraph.SCC[v].HasSubsetConditions() {
			// If the first vertex has subset conditions, then add it into graph.
			sscsSubGraph[v] = nil
		}
		s.searchTablesGraphDFS(v, from, sscsSubGraph)
		s.sccsGraph[v] = sscsSubGraph
	}
}

// searchTablesGraphDFS - recursive DFS function that used for finding all the subset-affected SCC's.
func (s *Subset) searchTablesGraphDFS(
	v int,
	from *[]condensationgraph.Edge,
	subGraph map[int][]condensationgraph.Edge,
) {
	for _, to := range s.condensationGraph.Graph[v] {
		// Add current edge to from list
		*from = append(*from, to)
		if s.condensationGraph.SCC[to.To().SCCID()].HasSubsetConditions() {
			// If SCC has subset condition, then dump it into sub graph
			dumpEdgesIntoGraph(subGraph, *from)
			// Clen up "from" slice in order to avoid duplicates in future edges dumps.
			*from = (*from)[:0]
		}
		s.searchTablesGraphDFS(v, from, subGraph)
		// Since the "from" slice can be cleaned on edges dump, we should check if it's 0
		// in order to avoid panic
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
}

// dumpEdgesIntoGraph - dumps the list of edges into sub-graph.
//
// It generates panic if the edges was already added, because they are unique, and this is definitely a bug
func dumpEdgesIntoGraph(graph map[int][]condensationgraph.Edge, edges []condensationgraph.Edge) {
	for _, e := range edges {
		if existingEdges, ok := graph[e.From().SCCID()]; ok {
			// Check if this edges is already dumped. If it's dumped - panic. This might be helpful to find
			// bugs where dumpEdgesIntoGraph function is not correctly called or graph map contains wrong vertexes
			// and edges.
			found := slices.ContainsFunc(existingEdges, func(edge condensationgraph.Edge) bool {
				if edge.ID() == e.ID() {
					return true
				}
				return false
			})
			if found {
				panic(
					fmt.Sprintf(
						"the concdesed edge %d is going to be added again %v+",
						e.ID(), e,
					),
				)
			}
		}
		graph[e.From().SCCID()] = append(graph[e.From().SCCID()], e)
	}
}
