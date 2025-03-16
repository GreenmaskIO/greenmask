package tablegraph

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common"
)

// Key - represents a simple primary key or foreign key item. Depending on the context it can represent a column
// or expression (e.g. get some value from the JSONB column).
type Key struct {
	Name       string
	Expression string
}

func NewKeysByColumn(cols []string) []Key {
	keys := make([]Key, 0, len(cols))
	for _, col := range cols {
		keys = append(keys, Key{Name: col})
	}
	return keys
}

//
//func NewKeysByReferencedColumn(cols []*domains.ReferencedColumn) []*Key {
//	keys := make([]*Key, 0, len(cols))
//	for _, col := range cols {
//		keys = append(keys, &Key{Name: col.Name, Expression: col.Expression})
//	}
//	return keys
//}

func (k *Key) GetKeyReference(t common.Table) string {
	if k.Expression != "" {
		return k.Expression
	}
	return fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, k.Name)
}
