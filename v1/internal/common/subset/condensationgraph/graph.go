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
	"slices"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
)

const (
	sccVertexIsVisited    = 1
	sccVertexIsNotVisited = -1
)

// Graph - the Graph representation of the DB vertexes. Is responsible for finding the cycles in the Graph
// and searching subset Path for the vertexes
type Graph struct {
	// tg - the table Graph. It contains the oriented Graph representation of the DB vertexes.
	tg tablegraph.Graph
	// SCC - the strongly connected components in the Graph
	SCC []SCC
	// Graph - the condensed Graph representation of the DB vertexes
	Graph [][]Edge
	// TransposedGraph - the reversed condensed Graph representation of the DB vertexes
	TransposedGraph [][]Edge
	// paths - the subset paths for the vertexes. The key is the vertex index in the Graph and the value is the path for
	// creating the subset query
	//paths    map[int]*Path
	visited []int
	// order - the topological order of the Graph. It is used to find the SCCs in the Graph.
	order    []int
	sccCount int
	// condensedEdges - the edges that are part of the condensed Graph. In case from and to parts in edge
	// contains condensed vertexes - this vertex can't be edge of condensed Graph. Instead, this edge should be
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

// buildSCC - builds the strongly connected components in the Graph.
//
// It uses the visited array to aggregate the vertexes for each SCC. Then it finds the edges within the SCC.
// The result is a list of SCC instances.
func (g *Graph) buildSCC() {
	// sccToOriginalVertexes - the mapping condensed Graph vertexes to the original Graph vertexes
	sccToOriginalVertexes := make(map[int][]int, g.sccCount)
	for vertexIdx, componentIdx := range g.visited {
		sccToOriginalVertexes[componentIdx] = append(sccToOriginalVertexes[componentIdx], vertexIdx)
	}

	for sccIdx := 0; sccIdx < g.sccCount; sccIdx++ {
		// Build list of vertexes for the SCC
		vertexes := make(map[int]commonmodels.Table)
		for _, vertexIdx := range sccToOriginalVertexes[sccIdx] {
			vertexes[vertexIdx] = g.tg.Vertexes[vertexIdx]
		}

		// Create an internal Graph of the SCC
		sccGraph := make(map[int][]tablegraph.Edge)
		for _, vertexIdx := range sccToOriginalVertexes[sccIdx] {
			// Get each vertex from the grouped SCC and find the edges that are connected to the vertex.
			var edges []tablegraph.Edge
			for _, e := range g.tg.Graph[vertexIdx] {
				if slices.Contains(sccToOriginalVertexes[sccIdx], e.To().TableID()) {
					// In case TO vertex is in the same SCC, we should add this edge to the SCC Graph/
					edges = append(edges, e)
					// Add the edge to the condensedEdges so it won't be part of the condensed Graph
					// Later if this edge ID will be found in the map it will be skipped.
					g.condensedEdges[e.ID()] = struct{}{}
				}
			}
			sccGraph[vertexIdx] = edges
		}

		g.SCC = append(g.SCC, NewSCC(sccIdx, sccGraph, vertexes))
	}
}

// buildSCCGraph - builds the condensed Graph.
//
// It uses the condensedEdges map to skip the edges that are part of the condensed Graph.
func (g *Graph) buildSCCGraph() {
	// 3. Build condensed Graph
	g.Graph = make([][]Edge, g.sccCount)
	g.TransposedGraph = make([][]Edge, g.sccCount)
	var condensedEdgeIdxSeq int
	for v := range g.tg.Graph {
		for _, edge := range g.tg.Graph[v] {
			if _, ok := g.condensedEdges[edge.ID()]; ok {
				continue
			}

			fromLinkIdx := g.visited[edge.From().TableID()]
			fromLink := NewLink(
				fromLinkIdx,
				g.SCC[fromLinkIdx],
			)
			toLinkIdx := g.visited[edge.To().TableID()]
			toLink := NewLink(
				toLinkIdx,
				g.SCC[toLinkIdx],
			)
			condensedEdge := NewEdge(condensedEdgeIdxSeq, fromLink, toLink, edge)
			g.Graph[fromLinkIdx] = append(g.Graph[fromLinkIdx], condensedEdge)
			reversedEdges := NewEdge(condensedEdgeIdxSeq, toLink, fromLink, edge)
			g.TransposedGraph[toLinkIdx] = append(g.TransposedGraph[toLinkIdx], reversedEdges)
			condensedEdgeIdxSeq++
		}
	}

}

// findSCC - finds the strongly connected components in the Graph.
//
// This is commonmodels Kosaraju's algorithm for finding the strongly connected components in the Graph.
//
// 1. Find the topological order of the Graph using DFS.
// 2. Reverse the Graph.
// 3. Mark the components using DFS. Each SCC will have a unique identifier.
// 4. Count the components.
//
// Once components are marked in visited array, we can use this information to build the condensed Graph.
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

// topologicalSortDfs - recursive function to visit all vertices of the Graph.
func (g *Graph) topologicalSortDfs(v int) {
	g.visited[v] = sccVertexIsVisited
	for _, to := range g.tg.Graph[v] {
		if g.visited[to.To().TableID()] == sccVertexIsNotVisited {
			g.topologicalSortDfs(to.To().TableID())
		}
	}
	g.order = append(g.order, v)
}

// markComponentDfs - recursive function to mark the components in the Graph.
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

func (g *Graph) HasCycles() bool {
	for _, scc := range g.SCC {
		if scc.HasCycle() {
			return true
		}
	}
	return false
}

func (g *Graph) GetTopologicalOrder() ([]int, error) {
	if g.HasCycles() {
		return nil, commonmodels.ErrTableGraphHasCycles
	}
	res := slices.Clone(g.order)
	slices.Reverse(res)
	return res, nil
}
