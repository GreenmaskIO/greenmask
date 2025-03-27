package condensationgraph

import "github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"

// Edge - represents an edge in the condensation Graph.
//
// It encapsulates the original edge from the table Graph and the condensed vertexes.
type Edge struct {
	// id - unique identifier of the edge.
	id int
	// from - the left part of the edge.
	from Link
	// to - the right part of the edge.
	to Link
	// originalEdge - the original edge from the table Graph.
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

func (e Edge) ID() int {
	return e.id
}

func (e Edge) From() Link {
	return e.from
}

func (e Edge) To() Link {
	return e.to
}

func (e Edge) OriginalEdge() tablegraph.Edge {
	return e.originalEdge
}
