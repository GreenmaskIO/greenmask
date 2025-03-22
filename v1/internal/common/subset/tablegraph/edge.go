package tablegraph

import (
	"fmt"
)

// Edge - represents an edge in the direct Graph. It contains the information about the edge itself and the
// information about the Vertexes that are connected by this edge.
type Edge struct {
	// id - the unique identifier of the edge.
	id int
	// isNullable - indicates if the edge is nullable. Meaning it requires to check that the join is either
	// connected by the values or by the NULL values. For instance if we have NULL value in the foreign key
	// we have to allow this row.
	isNullable bool
	// from - the left table in the Graph.
	from TableLink
	// to - the right table in the Graph.
	to TableLink
}

// NewEdge - creates a new Edge instance.
func NewEdge(id int, isNullable bool, a TableLink, b TableLink) Edge {
	return Edge{
		id:         id,
		isNullable: isNullable,
		from:       a,
		to:         b,
	}
}

// ID - returns the unique identifier of the edge.
func (e Edge) ID() int {
	return e.id
}

// IsNullable - indicates if the edge is nullable.
func (e Edge) IsNullable() bool {
	return e.isNullable
}

// From - returns the left table in the Graph.
func (e Edge) From() TableLink {
	return e.from
}

// To - returns the right table in the Graph.
func (e Edge) To() TableLink {
	return e.to
}

func (e Edge) DebugString() string {
	return fmt.Sprintf(
		"Edge[from=%s.%s to=%s.%s id=%d isNull=%t]",
		e.from.table.Schema, e.from.table.Name,
		e.to.table.Schema, e.to.table.Name,
		e.id, e.isNullable,
	)
}
