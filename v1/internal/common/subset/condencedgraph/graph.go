package condencedgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
	"slices"
)

const (
	sscVertexIsVisited    = 1
	sscVertexIsNotVisited = -1
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
	// sscToOriginalVertexes - the mapping condensed graph vertexes to the original graph vertexes
	sscToOriginalVertexes map[int][]int
	// paths - the subset paths for the vertexes. The key is the vertex index in the graph and the value is the path for
	// creating the subset query
	//paths    map[int]*Path
	edges    []tablegraph.Edge
	visited  []int
	order    []int
	sscCount int
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
	g.sscToOriginalVertexes = make(map[int][]int, g.sscCount)
	for vertexIdx, componentIdx := range g.visited {
		g.sscToOriginalVertexes[componentIdx] = append(g.sscToOriginalVertexes[componentIdx], vertexIdx)
	}
	// 1. Collect all vertexes for the component
	// 2. Find all edges within the component
	for sscIdx := 0; sscIdx < g.sscCount; sscIdx++ {
		vertexes := make(map[int]common.Table)
		for _, vertexIdx := range g.sscToOriginalVertexes[sscIdx] {
			vertexes[vertexIdx] = g.tg.Vertexes[vertexIdx]
		}

		sscGraph := make(map[int][]tablegraph.Edge)
		for _, vertexIdx := range g.sscToOriginalVertexes[sscIdx] {
			var edges []tablegraph.Edge
			for _, e := range g.tg.Graph[vertexIdx] {
				if slices.Contains(g.sscToOriginalVertexes[sscIdx], e.To().Index()) {
					edges = append(edges, e)
					g.condensedEdges[e.Index()] = struct{}{}
				}
			}
			sscGraph[vertexIdx] = edges
		}

		g.scc = append(g.scc, NewSCC(sscIdx, sscGraph, vertexes))
	}
}

func (g *Graph) buildSCCGraph() {

	// 3. Build condensed graph
	g.graph = make([][]Edge, g.sscCount)
	g.reversedCondensedGraph = make([][]Edge, g.sscCount)
	var condensedEdgeIdxSeq int
	for _, edge := range g.edges {
		if _, ok := g.condensedEdges[edge.ID()]; ok {
			continue
		}

		fromLinkIdx := g.visited[edge.From().Index()]
		fromLink := NewComponentLink(
			fromLinkIdx,
			g.scc[fromLinkIdx],
		)
		toLinkIdx := g.visited[edge.To().Index()]
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

// findSCC - finds the strongly connected components in the graph
func (g *Graph) findSCC() {
	g.order = g.order[:0]
	g.eraseVisited()
	for v := range g.tg.Graph {
		if g.visited[v] == sscVertexIsNotVisited {
			g.topologicalSortDfs(v)
		}
	}
	slices.Reverse(g.order)

	g.eraseVisited()
	var sscCount int
	for _, v := range g.order {
		if g.visited[v] == sscVertexIsNotVisited {
			g.markComponentDfs(v, sscCount)
			sscCount++
		}
	}
	g.sscCount = sscCount
}

func (g *Graph) eraseVisited() {
	for idx := range g.visited {
		g.visited[idx] = sscVertexIsNotVisited
	}
}

func (g *Graph) topologicalSortDfs(v int) {
	g.visited[v] = sscVertexIsVisited
	for _, to := range g.tg.Graph[v] {
		if g.visited[to.Index()] == sscVertexIsNotVisited {
			g.topologicalSortDfs(to.Index())
		}
	}
	g.order = append(g.order, v)
}

func (g *Graph) markComponentDfs(v, component int) {
	g.visited[v] = component
	for _, to := range g.reversedSimpleGraph[v] {
		if g.visited[to] == sscVertexIsNotVisited {
			g.markComponentDfs(to, component)
		}
	}
}
