package subset

import "github.com/greenmaskio/greenmask/internal/db/postgres/entries"

type TableLink struct {
	idx   int
	table *entries.Table
	keys  []string
}

func NewTableLink(idx int, t *entries.Table, keys []string) *TableLink {
	return &TableLink{
		idx:   idx,
		table: t,
		keys:  keys,
	}
}
