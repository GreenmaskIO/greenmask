package subset

import "slices"

const rootScopeId = 0

type Path struct {
	RootVertex int
	// Vertexes contains all the vertexes that are in the subset of the RootVertex vertex
	Vertexes []int
	// ScopeEdges - edges that are in the same scope with proper order
	ScopeEdges map[int][]*Edge
	// ScopeGraph - graph scope to scope connections
	ScopeGraph      map[int][]int
	Edges           []*Edge
	CycledEdges     map[int][]int
	scopeIdSequence int
}

func NewPath(rootVertex int) *Path {
	return &Path{
		RootVertex:      rootVertex,
		CycledEdges:     make(map[int][]int),
		ScopeGraph:      make(map[int][]int),
		ScopeEdges:      make(map[int][]*Edge),
		scopeIdSequence: rootScopeId,
	}
}

func (p *Path) AddVertex(v int) {
	p.Vertexes = append(p.Vertexes, v)
}

// AddEdge adds the edge to the path and return it scope
func (p *Path) AddEdge(e *Edge, scopeId int) int {
	if len(p.Vertexes) == 0 {
		// if there are no vertexes in the path, add the first (root) vertex
		p.AddVertex(e.A.Idx)
	}
	return p.addEdge(e, scopeId)
}

func (p *Path) MarkEdgeCycled(id int) {
	p.CycledEdges[id] = []int{}
}

func (p *Path) Len() int {
	return len(p.Vertexes)
}

func (p *Path) addEdge(e *Edge, scopeId int) int {
	if scopeId > p.scopeIdSequence {
		panic("scopeId is greater than the sequence")
	}
	p.createScopeIfNotExist(scopeId)

	// If the vertex is already in the scope then fork the scope and put the edge in the new scope
	if slices.ContainsFunc(p.ScopeEdges[scopeId], func(edge *Edge) bool {
		return edge.A.Idx == e.B.Idx || edge.B.Idx == e.B.Idx
	}) {
		p.scopeIdSequence++
		parestScopeId := scopeId
		scopeId = p.scopeIdSequence
		p.createScopeWithParent(parestScopeId, scopeId)
	}

	p.ScopeEdges[scopeId] = append(p.ScopeEdges[scopeId], e)
	p.Edges = append(p.Edges, e)
	p.Vertexes = append(p.Vertexes, e.B.Idx)
	return scopeId
}

func (p *Path) createScopeIfNotExist(scopeId int) {
	if _, ok := p.ScopeEdges[scopeId]; !ok {
		p.ScopeEdges[scopeId] = nil
		p.ScopeGraph[scopeId] = nil
	}
}

func (p *Path) createScopeWithParent(parentScopeId, scopeId int) {
	if _, ok := p.ScopeEdges[scopeId]; ok {
		panic("scope already exists")
	}
	// Create empty new scope
	p.ScopeEdges[scopeId] = nil
	p.ScopeGraph[scopeId] = nil
	// Add the new scope to the parent scope
	p.ScopeGraph[parentScopeId] = append(p.ScopeGraph[parentScopeId], scopeId)
}
