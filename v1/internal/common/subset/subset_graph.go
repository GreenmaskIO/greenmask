package subset

import (
	"errors"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condensationgraph"
)

var (
	errEdgeIsNotUnique = errors.New("the edge is not unique")
)

type subsetGraph struct {
	graph      map[int][]condensationgraph.Edge
	rootVertex int
	// vertexes - is a list of vertexes (SCC) in the graph.
	// The key is the vertex ID (SCC ID) and the value is the SCC instance.
	vertexes map[int]condensationgraph.SCC
}

// subsetGraph - represents the graph that was render specifically for some vertex (table).
func newSubsetGraph(rootVertex int) *subsetGraph {
	return &subsetGraph{
		graph:      make(map[int][]condensationgraph.Edge),
		rootVertex: rootVertex,
		vertexes:   make(map[int]condensationgraph.SCC),
	}
}

// validateEdgeIsUnique - Checks if this edges is already dumped. If it's dumped - it returns and error.
// This might be helpful to find bugs where dumpEdgesIntoGraph function is not correctly called or
// graph map contains wrong vertexes and edges.
func (g *subsetGraph) validateEdgeIsUnique(e condensationgraph.Edge) error {
	existingEdges, ok := g.graph[e.From().SCC.ID()]
	if !ok {
		// If the edge is not in the graph, then we can addEdge it.
		return nil
	}

	// Check if this edges is already dumped
	found := slices.ContainsFunc(existingEdges, func(edge condensationgraph.Edge) bool {
		return edge.ID() == e.ID()
	})
	if found {
		// If it has been already dumped it might be a bug - return an error.
		return fmt.Errorf(
			"bug is detected concdesed edge %d is going to be added again %v: %w",
			e.ID(), e, errEdgeIsNotUnique,
		)
	}
	return nil
}

// addVertex - adds the vertex to the graph. Since for the whole subset graph there might be
// alone vertex (SCC) without any edges, we need to add it's SCC ID and the SCC itself.
//
// It just simply add vertex without any connection to other vertexes.
// You can use this method in case you want to add vertex without any edges.
// In subset graph it requires only for adding rootVertex itself.
func (g *subsetGraph) addVertex(v int, scc condensationgraph.SCC) {
	g.graph[v] = nil
	g.vertexes[v] = scc
}

// addEdge - adds the edge to the graph.
func (g *subsetGraph) addEdge(e condensationgraph.Edge) {
	// Check if the edge is already in the graph.
	if err := g.validateEdgeIsUnique(e); err != nil {
		// If the edge is already in the graph it supposed to be a bug.
		panic(fmt.Errorf("validate edge is unique: %w", err))
	}
	// It appends the edge to the graph using the Form SCC as a key for list of connections.
	// The edge itself contains the To and From vertexes, so we can use it to find the correct SCC.
	g.graph[e.From().SCC.ID()] = append(g.graph[e.From().SCC.ID()], e)
	// Add the empty To vertex to the graph.
	// Since we need to recall all the vertexes in the graph that are involved in the subset.
	g.graph[e.To().SCC.ID()] = nil
	// Add the edge to the vertexes map for the From and To vertexes.
	if _, ok := g.vertexes[e.From().SCC.ID()]; !ok {
		g.vertexes[e.From().SCC.ID()] = e.From().SCC
	}
	if _, ok := g.vertexes[e.To().SCC.ID()]; !ok {
		g.vertexes[e.To().SCC.ID()] = e.To().SCC
	}
}

// hasCycles - returns true if the graph has at least one cycle.
func (g *subsetGraph) hasCycles() bool {
	for _, edges := range g.graph {
		for _, e := range edges {
			if e.From().SCC.HasCycle() {
				return true
			}
		}
	}
	return false
}

// vertexes - returns count of vertexes in the graph.
func (g *subsetGraph) vertexCount() int {
	return len(g.vertexes)
}
