package condensationgraph

import "github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"

// Edge - represents an edge in the condensation graph.
//
// It encapsulates the original edge from the table graph and the condensed vertexes.
type Edge struct {
	// id - unique identifier of the edge.
	id int
	// from - the left part of the edge.
	from Link
	// to - the right part of the edge.
	to Link
	// originalEdge - the original edge from the table graph.
	originalEdge tablegraph.Edge
}

// NewEdge - creates a new Edge instance.
func NewEdge(id int, from, to Link, originalEdge tablegraph.Edge) Edge {
	return Edge{
		id:           id,
		from:         from,
		to:           to,
		originalEdge: originalEdge,
	}
}
