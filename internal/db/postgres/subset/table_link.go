package subset

import "github.com/greenmaskio/greenmask/internal/db/postgres/entries"

type TableLink struct {
	Idx   int
	Table *entries.Table
	Keys  []string
}

func NewTableLink(idx int, t *entries.Table, keys []string) *TableLink {
	return &TableLink{
		Idx:   idx,
		Table: t,
		Keys:  keys,
	}
}
