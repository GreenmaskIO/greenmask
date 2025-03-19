package tablegraph

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common"
)

// TableLink - a right or left table that contains all the required data to make a join between two Vertexes.
type TableLink struct {
	// idx - the index of the table in the Graph.
	idx int
	// table - the table itself.
	table common.Table
	// keys - the keys that are used to join this specific table with another table.
	keys []Key
	// polymorphicExpressions - polymorphic expressions for single conditions that are not used to match FK and
	// PK values this might be used for polymorphic relations
	polymorphicExpressions []string
}

// NewTableLink - creates a new TableLink instance.
func NewTableLink(
	idx int,
	table common.Table,
	keys []Key,
	polymorphicExpressions []string,
) TableLink {
	return TableLink{
		idx:                    idx,
		table:                  table,
		keys:                   keys,
		polymorphicExpressions: polymorphicExpressions,
	}
}

// Index - returns the index of the table in the Graph.
func (tl TableLink) Index() int {
	return tl.idx
}

// Table - returns the table itself.
func (tl TableLink) Table() common.Table {
	return tl.table
}

func (tl TableLink) GetTableName() string {
	return fmt.Sprintf("%s.%s", tl.table.Schema, tl.table.Name)
}

// Keys - returns the keys that are used to join this specific table with another table.
func (tl TableLink) Keys() []Key {
	return tl.keys
}

// PolymorphicExpressions - returns polymorphic expressions for single conditions that are not used to match FK and
func (tl TableLink) PolymorphicExpressions() []string {
	return tl.polymorphicExpressions
}
