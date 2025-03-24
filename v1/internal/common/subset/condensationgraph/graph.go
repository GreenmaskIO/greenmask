package condensationgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
	"slices"
)

const (
	sccVertexIsVisited    = 1
	sccVertexIsNotVisited = -1
)

// Graph - the graph representation of the DB vertexes. Is responsible for finding the cycles in the graph
// and searching subset Path for the vertexes
type Graph struct {
	// tg - the table graph. It contains the oriented graph representation of the DB vertexes.
	tg tablegraph.Graph
	// scc - the strongly connected components in the graph
	scc []SCC
	// graph - the condensed graph representation of the DB vertexes
	graph [][]Edge
	// transposedGraph - the reversed condensed graph representation of the DB vertexes
	transposedGraph [][]Edge
	// paths - the subset paths for the vertexes. The key is the vertex index in the graph and the value is the path for
	// creating the subset query
	//paths    map[int]*Path
	visited  []int
	order    []int
	sccCount int
	// condensedEdges - the edges that are part of the condensed graph. In case from and to parts in edge
	// contains condensed vertexes - this vertex can't be edge of condensed graph. Instead, this edge should be
	// inside CycleGraph
	condensedEdges map[int]struct{}
}

func NewGraph(tg tablegraph.Graph) Graph {
	g := Graph{
		tg:             tg,
		condensedEdges: make(map[int]struct{}),
		visited:        make([]int, len(tg.Vertexes)),
	}
	g.build()
	return g
}

func (g *Graph) build() {
	g.findSCC()
	g.buildSCC()
	g.buildSCCGraph()
}

// buildSCC - builds the strongly connected components in the graph.
//
// It uses the visited array to aggregate the vertexes for each ssc. Then it finds the edges within the ssc.
// The result is a list of SCC instances.
func (g *Graph) buildSCC() {
	// sccToOriginalVertexes - the mapping condensed graph vertexes to the original graph vertexes
	sccToOriginalVertexes := make(map[int][]int, g.sccCount)
	for vertexIdx, componentIdx := range g.visited {
		sccToOriginalVertexes[componentIdx] = append(sccToOriginalVertexes[componentIdx], vertexIdx)
	}

	for sccIdx := 0; sccIdx < g.sccCount; sccIdx++ {
		// Build list of vertexes for the SCC
		vertexes := make(map[int]common.Table)
		for _, vertexIdx := range sccToOriginalVertexes[sccIdx] {
			vertexes[vertexIdx] = g.tg.Vertexes[vertexIdx]
		}

		// Create an internal graph of the SCC
		sccGraph := make(map[int][]tablegraph.Edge)
		for _, vertexIdx := range sccToOriginalVertexes[sccIdx] {
			// Get each vertex from the grouped SCC and find the edges that are connected to the vertex.
			var edges []tablegraph.Edge
			for _, e := range g.tg.Graph[vertexIdx] {
				if slices.Contains(sccToOriginalVertexes[sccIdx], e.To().TableID()) {
					// In case TO vertex is in the same SCC, we should add this edge to the SCC graph/
					edges = append(edges, e)
					// Add the edge to the condensedEdges so it won't be part of the condensed graph
					// Later if this edge ID will be found in the map it will be skipped.
					g.condensedEdges[e.ID()] = struct{}{}
				}
			}
			sccGraph[vertexIdx] = edges
		}

		g.scc = append(g.scc, NewSCC(sccIdx, sccGraph, vertexes))
	}
}

// buildSCCGraph - builds the condensed graph.
//
// It uses the condensedEdges map to skip the edges that are part of the condensed graph.
func (g *Graph) buildSCCGraph() {
	// 3. Build condensed graph
	g.graph = make([][]Edge, g.sccCount)
	g.transposedGraph = make([][]Edge, g.sccCount)
	var condensedEdgeIdxSeq int
	for v := range g.tg.Graph {
		for _, edge := range g.tg.Graph[v] {
			if _, ok := g.condensedEdges[edge.ID()]; ok {
				continue
			}

			fromLinkIdx := g.visited[edge.From().TableID()]
			fromLink := NewLink(
				fromLinkIdx,
				g.scc[fromLinkIdx],
			)
			toLinkIdx := g.visited[edge.To().TableID()]
			toLink := NewLink(
				toLinkIdx,
				g.scc[toLinkIdx],
			)
			condensedEdge := NewEdge(condensedEdgeIdxSeq, fromLink, toLink, edge)
			g.graph[fromLinkIdx] = append(g.graph[fromLinkIdx], condensedEdge)
			reversedEdges := NewEdge(condensedEdgeIdxSeq, toLink, fromLink, edge)
			g.transposedGraph[toLinkIdx] = append(g.transposedGraph[toLinkIdx], reversedEdges)
			condensedEdgeIdxSeq++
		}
	}

}

// findSCC - finds the strongly connected components in the graph.
//
// This is common Kosaraju's algorithm for finding the strongly connected components in the graph.
//
// 1. Find the topological order of the graph using DFS.
// 2. Reverse the graph.
// 3. Mark the components using DFS. Each ssc will have a unique identifier.
// 4. Count the components.
//
// Once components are marked in visited array, we can use this information to build the condensed graph.
func (g *Graph) findSCC() {
	g.order = g.order[:0]
	g.eraseVisited()
	for v := range g.tg.Graph {
		if g.visited[v] == sccVertexIsNotVisited {
			g.topologicalSortDfs(v)
		}
	}
	slices.Reverse(g.order)

	g.eraseVisited()
	var sccCount int
	for _, v := range g.order {
		if g.visited[v] == sccVertexIsNotVisited {
			g.markComponentDfs(v, sccCount)
			sccCount++
		}
	}
	g.sccCount = sccCount
}

// eraseVisited - erases the visited array.
//
// It sets all the values to sccVertexIsNotVisited.
func (g *Graph) eraseVisited() {
	for idx := range g.visited {
		g.visited[idx] = sccVertexIsNotVisited
	}
}

// topologicalSortDfs - recursive function to visit all vertices of the graph.
func (g *Graph) topologicalSortDfs(v int) {
	g.visited[v] = sccVertexIsVisited
	for _, to := range g.tg.Graph[v] {
		if g.visited[to.To().TableID()] == sccVertexIsNotVisited {
			g.topologicalSortDfs(to.To().TableID())
		}
	}
	g.order = append(g.order, v)
}

// markComponentDfs - recursive function to mark the components in the graph.
//
// It marks the vertexes with the component identifier in visited array.
func (g *Graph) markComponentDfs(v, component int) {
	g.visited[v] = component
	for _, e := range g.tg.TransposedGraph[v] {
		if g.visited[e.To().TableID()] == sccVertexIsNotVisited {
			g.markComponentDfs(e.To().TableID(), component)
		}
	}
}
