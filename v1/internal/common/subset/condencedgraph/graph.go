package condencedgraph

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
	// graph - the oriented graph representation of the DB vertexes
	reversedSimpleGraph [][]int
	// scc - the strongly connected components in the graph
	scc []SCC
	// graph - the condensed graph representation of the DB vertexes
	graph [][]Edge
	// reversedCondensedGraph - the reversed condensed graph representation of the DB vertexes
	reversedCondensedGraph [][]Edge
	// sccToOriginalVertexes - the mapping condensed graph vertexes to the original graph vertexes
	sccToOriginalVertexes map[int][]int
	// paths - the subset paths for the vertexes. The key is the vertex index in the graph and the value is the path for
	// creating the subset query
	//paths    map[int]*Path
	edges    []tablegraph.Edge
	visited  []int
	order    []int
	sccCount int
	// condensedEdges - the edges that are part of the condensed graph
	// TODO: Clarify what is that
	condensedEdges map[int]struct{}
}

func NewGraph(tg tablegraph.Graph) Graph {
	g := Graph{
		tg:             tg,
		condensedEdges: make(map[int]struct{}),
	}
	g.buildGraph()
	return g
}

func (g *Graph) buildGraph() {
	g.findSCC()
	g.buildSCC()
	g.buildSCCGraph()
}

func (g *Graph) buildSCC() {
	g.sccToOriginalVertexes = make(map[int][]int, g.sccCount)
	for vertexIdx, componentIdx := range g.visited {
		g.sccToOriginalVertexes[componentIdx] = append(g.sccToOriginalVertexes[componentIdx], vertexIdx)
	}
	// 1. Collect all vertexes for the component
	// 2. Find all edges within the component
	for sccIdx := 0; sccIdx < g.sccCount; sccIdx++ {
		vertexes := make(map[int]common.Table)
		for _, vertexIdx := range g.sccToOriginalVertexes[sccIdx] {
			vertexes[vertexIdx] = g.tg.Vertexes[vertexIdx]
		}

		sccGraph := make(map[int][]tablegraph.Edge)
		for _, vertexIdx := range g.sccToOriginalVertexes[sccIdx] {
			var edges []tablegraph.Edge
			for _, e := range g.tg.Graph[vertexIdx] {
				if slices.Contains(g.sccToOriginalVertexes[sccIdx], e.To().TableID()) {
					edges = append(edges, e)
					g.condensedEdges[e.From().TableID()] = struct{}{}
				}
			}
			sccGraph[vertexIdx] = edges
		}

		g.scc = append(g.scc, NewSCC(sccIdx, sccGraph, vertexes))
	}
}

func (g *Graph) buildSCCGraph() {

	// 3. Build condensed graph
	g.graph = make([][]Edge, g.sccCount)
	g.reversedCondensedGraph = make([][]Edge, g.sccCount)
	var condensedEdgeIdxSeq int
	for _, edge := range g.edges {
		if _, ok := g.condensedEdges[edge.ID()]; ok {
			continue
		}

		fromLinkIdx := g.visited[edge.From().TableID()]
		fromLink := NewComponentLink(
			fromLinkIdx,
			g.scc[fromLinkIdx],
		)
		toLinkIdx := g.visited[edge.To().TableID()]
		toLink := NewComponentLink(
			toLinkIdx,
			g.scc[toLinkIdx],
		)
		condensedEdge := NewEdge(condensedEdgeIdxSeq, fromLink, toLink, edge)
		g.graph[fromLinkIdx] = append(g.graph[fromLinkIdx], condensedEdge)
		reversedEdges := NewEdge(condensedEdgeIdxSeq, toLink, fromLink, edge)
		g.reversedCondensedGraph[toLinkIdx] = append(g.reversedCondensedGraph[toLinkIdx], reversedEdges)
		condensedEdgeIdxSeq++
	}
}

// findSCC - finds the strongly connected components in the graph.
//
// This is common Kosaraju's algorithm for finding the strongly connected components in the graph.
//
// 1. Find the topological order of the graph using DFS.
// 2. Reverse the graph.
// 3. Mark the components using DFS. Each component will have a unique identifier.
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
		if g.visited[to.From().TableID()] == sccVertexIsNotVisited {
			g.topologicalSortDfs(to.From().TableID())
		}
	}
	g.order = append(g.order, v)
}

func (g *Graph) markComponentDfs(v, component int) {
	g.visited[v] = component
	for _, to := range g.reversedSimpleGraph[v] {
		if g.visited[to] == sccVertexIsNotVisited {
			g.markComponentDfs(to, component)
		}
	}
}
