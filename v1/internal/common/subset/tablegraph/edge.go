package tablegraph

// Edge - represents an edge in the direct Graph. It contains the information about the edge itself and the
// information about the Vertexes that are connected by this edge.
type Edge struct {
	// id - the unique identifier of the edge.
	id int
	// idx - the index of the left table in the Graph.
	idx int
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
func NewEdge(id, idx int, isNullable bool, a TableLink, b TableLink) Edge {
	return Edge{
		id:         id,
		idx:        idx,
		isNullable: isNullable,
		from:       a,
		to:         b,
	}
}

// ID - returns the unique identifier of the edge.
func (e Edge) ID() int {
	return e.id
}

// Index - returns the index of the left table in the Graph.
func (e Edge) Index() int {
	return e.idx
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
