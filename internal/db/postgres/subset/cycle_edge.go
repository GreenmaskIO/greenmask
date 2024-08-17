package subset

import "github.com/greenmaskio/greenmask/internal/db/postgres/entries"

type CycleEdge struct {
	id     int
	from   string
	to     string
	tables []*entries.Table
}

func NewCycleEdge(id int, from, to string, tables []*entries.Table) *CycleEdge {
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
