package condencedgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
)

type CycleEdge struct {
	id     int
	from   string
	to     string
	tables []common.Table
}

func NewCycleEdge(id int, from, to string, tables []common.Table) *CycleEdge {
	if len(tables) == 0 {
		panic("empty tables provided for cycle edge")
	}
	return &CycleEdge{
		id:     id,
		from:   from,
		to:     to,
		tables: tables,
	}
}
