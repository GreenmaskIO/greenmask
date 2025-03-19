package subset

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/condencedgraph"
	"slices"
)

const rootScopeId = 0

type Path struct {
	rootVertex int
	// vertexes contains all the vertexes that are in the subset of the rootVertex vertex
	vertexes []int
	// scopeEdges - edges that are in the same scope with proper order
	scopeEdges         map[int][]condencedgraph.Edge
	scopeEdgesNullable map[int]map[int]bool
	// scopeGraph - graph scope to scope connections
	scopeGraph      map[int][]ScopeEdge
	edges           []condencedgraph.Edge
	graph           map[int][]condencedgraph.Edge
	scopeIdSequence int
}

func NewPath(rootVertex int) *Path {
	return &Path{
		rootVertex:         rootVertex,
		scopeGraph:         make(map[int][]ScopeEdge),
		scopeEdges:         make(map[int][]condencedgraph.Edge),
		scopeEdgesNullable: make(map[int]map[int]bool),
		scopeIdSequence:    rootScopeId,
		graph:              make(map[int][]condencedgraph.Edge),
	}
}

func (p *Path) AddVertex(v int) {
	p.vertexes = append(p.vertexes, v)
}

// AddEdge adds the edge to the path and return it scope
func (p *Path) AddEdge(e condencedgraph.Edge, scopeId int) int {
	p.addEdgeToGraph(e)
	if len(p.vertexes) == 0 {
		// if there are no vertexes in the path, add the first (root) vertex
		p.AddVertex(e.from.idx)
	}
	return p.addEdge(e, scopeId)
}

func (p *Path) addEdgeToGraph(e condencedgraph.Edge) {
	p.graph[e.from.idx] = append(p.graph[e.from.idx], e)
	if _, ok := p.graph[e.to.idx]; !ok {
		p.graph[e.to.idx] = nil
	}
}

func (p *Path) Len() int {
	return len(p.vertexes)
}

func (p *Path) addEdge(e condencedgraph.Edge, scopeId int) int {
	if scopeId > p.scopeIdSequence {
		panic("scopeId is greater than the sequence")
	}
	p.createScopeIfNotExist(scopeId)

	// If the vertex is already in the scope (or has cycle) then fork the scope and put the edge in the new scope
	if e.to.component.hasCycle() || vertexIsInScope(p.scopeEdges[scopeId], e) {
		scopeId = p.createScopeWithParent(scopeId, e)
	} else {
		isNullable := e.originalEdge.isNullable
		if !isNullable {
			isNullable = slices.ContainsFunc(p.scopeEdges[scopeId], func(edge *CondensedEdge) bool {
				return edge.originalEdge.to.idx == e.from.idx && edge.originalEdge.isNullable
			})
		}
		p.scopeEdgesNullable[scopeId][e.to.idx] = isNullable
		p.scopeEdges[scopeId] = append(p.scopeEdges[scopeId], e)
	}
	p.edges = append(p.edges, e)
	p.vertexes = append(p.vertexes, e.to.idx)
	return scopeId
}

func (p *Path) createScopeIfNotExist(scopeId int) {
	if _, ok := p.scopeEdges[scopeId]; !ok {
		p.scopeEdges[scopeId] = nil
		p.scopeGraph[scopeId] = nil
		p.scopeEdgesNullable[scopeId] = make(map[int]bool)
	}
}

func (p *Path) createScopeWithParent(parentScopeId int, e condencedgraph.Edge) int {
	p.scopeIdSequence++
	scopeId := p.scopeIdSequence

	if _, ok := p.scopeEdges[scopeId]; ok {
		panic("scope already exists")
	}
	// Create empty new scope
	p.scopeEdges[scopeId] = nil
	p.scopeGraph[scopeId] = nil
	p.scopeEdgesNullable[scopeId] = make(map[int]bool)
	p.scopeEdges[scopeId] = append(p.scopeEdges[scopeId], e)

	// Add the new scope to the parent scope
	isNullable := e.originalEdge.isNullable
	if !isNullable {
		isNullable = p.scopeEdgesNullable[parentScopeId][e.from.idx]
	}

	scopeEdge := &ScopeEdge{
		scopeId:               scopeId,
		originalCondensedEdge: e,
		isNullable:            isNullable,
	}
	p.scopeGraph[parentScopeId] = append(p.scopeGraph[parentScopeId], scopeEdge)
	return scopeId
}

func vertexIsInScope(scopeEdges []*CondensedEdge, e *CondensedEdge) bool {
	return slices.ContainsFunc(scopeEdges, func(edge *CondensedEdge) bool {
		return edge.from.idx == e.to.idx || edge.to.idx == e.to.idx
	})
}
