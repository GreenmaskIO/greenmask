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
	"fmt"
	"slices"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

// queryBuilder - returns built query for SCC.
//
// Since the root SCC may be a cycle and contain more tha one
// vertex (table) it return a map of vertexes to the query render.
// The key is TableID and value is a query for this table.
type queryBuilder interface {
	build() (map[int]string, error)
}

// The tasks to resolve
//
// FIND AFFECTED VERTEXES AND BUILD PATH
// 1. Build all graphs
// 2. Use Condensation graph for building a query path since it's a DAG
// 3. Store each query path to the list of queries

// I believe we can store the required sub-graph in map[int][]condensationgraph.Edge.
// If there is a table that has only one vertex without JOIN's then we will use only
// the vertex ID for a query

// Subset - represents a subset engine for the tables.
//
// It generates the subset queries for the tables.
type Subset struct {
	tables            []commonmodels.Table
	tableGraph        tablegraph.Graph
	condensationGraph condensationgraph.Graph
	// subsetGraphs - is a list of sub-graphs assigned to the specific SCC.
	// The index of slice represents SCC Idx, meaning you can find SCC
	// in SCC list by Idx.
	subsetGraphs  []*subsetGraph
	tablesQueries []string
	dialect       Dialect
}

// validateTablesHasUniqueIDs - validates that all tables have unique IDs.
// This will help you to debug cases when IDs are forgotten to be set.
func validateTablesHasUniqueIDs(tables []commonmodels.Table) {
	// Validate that all tables have unique IDs
	seen := make(map[int]struct{})
	for _, table := range tables {
		if _, exists := seen[table.ID]; exists {
			panic(fmt.Sprintf("table with ID %d already exists", table.ID))
		}
		seen[table.ID] = struct{}{}
	}
}

// NewSubset - creates a new Subset instance.
//
// TODO: I suspect the version of dialect is required as well but a bit later I decide to add it.
//
//	I don't know the version format for now
func NewSubset(tables []commonmodels.Table, dialect Dialect) (Subset, error) {
	validateTablesHasUniqueIDs(tables)
	tableGraph, err := tablegraph.NewGraph(tables)
	if err != nil {
		return Subset{}, fmt.Errorf("create table graph: %w", err)
	}
	condensationGraph := condensationgraph.NewGraph(tableGraph)
	s := Subset{
		tables:            tables,
		tableGraph:        tableGraph,
		condensationGraph: condensationGraph,
		subsetGraphs:      make([]*subsetGraph, len(condensationGraph.SCC)),
		tablesQueries:     make([]string, len(tables)),
		dialect:           dialect,
	}
	s.searchTablesGraph()
	if err := s.buildSubsetQueries(); err != nil {
		return Subset{}, fmt.Errorf("render subset queries: %w", err)
	}
	return s, nil
}

// searchTablesGraph - find sub-graphs for tables that has at least one subset condition in the path.
//
// Each key of the map represents the tableID that is the index of the table in the tables slice.
// The value is the subset query for the table.
func (s *Subset) searchTablesGraph() {
	// We start the DFS from each vertex.
	// When Subset condition is found we have to addEdge all edges start from the beginning
	// (or the previous vertex with subset). Those edges must be added as a graph.
	//
	//  For example
	// 		The graph 1 -> 2 -> 3 -> 4 -> 5 -> 6
	//		List of vertexes with subset: 3, 5
	//  	Then
	//			Once subset 3 is met - addEdge the edges with vertexes 1 -> 2 -> 3 into graph
	//				The result: 1 -> 2 -> 3
	//			Once subset 5 is met - addEdge the edges with vertexes 4 -> 5 into graph
	//				The result: 1 -> 2 -> 3 -> 4 -> 5
	//			The vertex 6 will be skipped because it does not affect the previous, because it does not has
	//			subset conditions.
	//
	// The resulting Sub-Graph will be used for query planning later.
	for v := range s.condensationGraph.Graph {
		var (
			from []condensationgraph.Edge
			// sg - the graph that contains all the vertexes (SCC) that are affected
			// by subset conditions. This will be used for query planning.
			sg = newSubsetGraph(v)
		)
		if s.condensationGraph.SCC[v].HasSubsetConditions() {
			// If the first vertex has subset conditions, then addEdge it into graph.
			sg.addVertex(v, s.condensationGraph.SCC[v])

		}
		s.searchTablesGraphDFS(v, &from, sg)
		if sg.vertexCount() != 0 {
			// If the sub-graph is not empty, then we have to store the graph for the vertex (SCC).
			s.subsetGraphs[v] = sg
		}
	}
}

// searchTablesGraphDFS - recursive DFS function that used for finding all the subset-affected SCC's.
func (s *Subset) searchTablesGraphDFS(
	v int,
	from *[]condensationgraph.Edge,
	subGraph *subsetGraph,
) {
	for _, to := range s.condensationGraph.Graph[v] {
		// Add current edge to from list
		*from = append(*from, to)
		if s.condensationGraph.SCC[to.To().SCC.ID()].HasSubsetConditions() {
			// If SCC has subset condition, then dump it into sub graph
			dumpEdgesIntoGraph(subGraph, *from)
			// Clen up "from" slice in order to avoid duplicates in future edges dumps.
			*from = (*from)[:0]
		}
		s.searchTablesGraphDFS(to.To().SCC.ID(), from, subGraph)
		// Since the "from" slice can be cleaned on edges dump, we should check if it's 0
		// in order to avoid panic
		if len(*from) > 0 {
			*from = (*from)[:len(*from)-1]
		}
	}
}

// dumpEdgesIntoGraph - dumps the list of edges into sub-graph.
//
// It generates panic if the edges was already added, because they are unique, and this is definitely a bug.
func dumpEdgesIntoGraph(graph *subsetGraph, edges []condensationgraph.Edge) {
	for _, e := range edges {
		graph.addEdge(e)
	}
}

// buildSubsetQueries - builds the queries for each SCC.
func (s *Subset) buildSubsetQueries() error {
	for _, sg := range s.subsetGraphs {
		if sg == nil || sg.vertexCount() == 0 {
			// If the sub-graph is empty, then we can skip it.
			continue
		}
		queries, err := s.buildQueryForSCC(sg)
		if err != nil {
			return fmt.Errorf("render query for SCC %d: %w", sg.rootVertex, err)
		}
		s.setQueryForEachTableOfSCC(queries)
	}
	return nil
}

// setQueryForEachTableOfSCC - sets the query for each table of SCC.
// It associates the generated query with list of tables.
func (s *Subset) setQueryForEachTableOfSCC(queries map[int]string) {
	for tableID, query := range queries {
		s.tablesQueries[tableID] = query
	}
}

// buildQueryForSCC - builds the query for the provides subsetGraph.
//
// Since the root vertex (root SCC) may be a cycle and contain more than one
// vertex (table) it returns a map of vertexes to the query render.
// The map key represents TableID and value is a query for this table.
func (s *Subset) buildQueryForSCC(sg *subsetGraph) (map[int]string, error) {
	var builder queryBuilder
	if sg.hasCycles() {
		// If the graph has cycles, then we need to use the cycle query builder.
		builder = newCyclesQueryBuilder(sg, s.dialect)
	} else {
		// If the graph is a DAG, then we need to use the DAG query builder.
		builder = newDAGQueryBuilder(sg, s.dialect)
	}
	queries, err := builder.build()
	if err != nil {
		return nil, err
	}
	return queries, nil
}

func (s *Subset) GetTableQueries() []string {
	return slices.Clone(s.tablesQueries)
}

func (s *Subset) GetTopologicalOrder() ([]int, error) {
	condGraphTopOrder, err := s.condensationGraph.GetTopologicalOrder()
	if err != nil {
		return nil, err
	}
	tableTopOrder := make([]int, 0, len(s.tables))
	for _, sccIdx := range condGraphTopOrder {
		vxs := s.condensationGraph.SCC[sccIdx].Vertexes()
		if len(vxs) == 0 {
			return nil, fmt.Errorf("SCC %d has no vertexes", sccIdx)
		}
		if len(vxs) > 1 {
			panic(fmt.Errorf("graph is not DAG: SCC %d has more than 1 vertex", sccIdx))
		}
		tableID := vxs[0].ID
		tableTopOrder = append(tableTopOrder, tableID)
	}
	slices.Reverse(tableTopOrder)
	return tableTopOrder, nil
}

func (s *Subset) GetTableGraph() tablegraph.Graph {
	return s.tableGraph
}
