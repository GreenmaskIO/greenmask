package condencedgraph

import "github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"

type Edge struct {
	id           int
	from         ComponentLink
	to           ComponentLink
	originalEdge tablegraph.Edge
}

func NewEdge(id int, from, to ComponentLink, originalEdge tablegraph.Edge) Edge {
	return Edge{
		id:           id,
		from:         from,
		to:           to,
		originalEdge: originalEdge,
	}
}
