package cyclesgraph

import "github.com/greenmaskio/greenmask/v1/internal/common"

type Edge struct {
	id     int
	from   string
	to     string
	tables []common.Table
}

func NewEdge(id int, from, to string, tables []common.Table) Edge {
	if len(tables) == 0 {
		panic("empty tables provided for cycle edge")
	}
	return Edge{
		id:     id,
		from:   from,
		to:     to,
		tables: tables,
	}
}
