package cyclesgraph

import "github.com/greenmaskio/greenmask/v1/internal/common"

// Edge - represents an edge in the graph in cycles.
//
// It connects two cycles via common vertexes. For example, we have two cycles 1->2->3 and 2->3->4.
// The common vertexes are 2 and 3.
// The from value will be 1_2_3 and the to value will be 2_3_4.
type Edge struct {
	// id - the unique identifier of the edge.
	id int
	// from - the from cycle identifier.
	from string
	// to - the to cycle identifier.
	to string
	// commonVertexes - the common vertexes that can be used to join the cycles.
	commonVertexes []common.Table
}

// NewEdge - creates a new Edge instance.
func NewEdge(id int, from, to string, tables []common.Table) Edge {
	if len(tables) == 0 {
		panic("empty commonVertexes provided for cycle edge")
	}
	return Edge{
		id:             id,
		from:           from,
		to:             to,
		commonVertexes: tables,
	}
}
