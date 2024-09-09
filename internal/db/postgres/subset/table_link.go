package subset

import (
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/domains"
)

type Key struct {
	Name       string
	Expression string
}

func (k *Key) GetKeyReference(t *entries.Table) string {
	if k.Expression != "" {
		return k.Expression
	}
	return fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, k.Name)
}

func NewKeysByColumn(cols []string) []*Key {
	keys := make([]*Key, 0, len(cols))
	for _, col := range cols {
		keys = append(keys, &Key{Name: col})
	}
	return keys
}

func NewKeysByReferencedColumn(cols []*domains.ReferencedColumn) []*Key {
	keys := make([]*Key, 0, len(cols))
	for _, col := range cols {
		keys = append(keys, &Key{Name: col.Name, Expression: col.Expression})
	}
	return keys
}

type TableLink struct {
	idx   int
	table *entries.Table
	keys  []*Key
	// polymorphicExprs - polymorphicExprs for single conditions that are not used to match FK and PK values
	// this might be used for polymorphic relations
	polymorphicExprs []string
}

func NewTableLink(idx int, t *entries.Table, keys []*Key, polymorphicExprs []string) *TableLink {
	return &TableLink{
		idx:              idx,
		table:            t,
		keys:             keys,
		polymorphicExprs: polymorphicExprs,
	}
}
