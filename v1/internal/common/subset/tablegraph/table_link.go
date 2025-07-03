package tablegraph

import (
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

// TableLink - a right or left table that contains all the required data to make a join between two Vertexes.
type TableLink struct {
	// ID - the index of the table in the Graph.
	ID int
	// table - the table itself.
	table commonmodels.Table
	// keys - the keys that are used to join this specific table with another table.
	keys []Key
	// polymorphicExpressions - polymorphic expressions for single conditions that are not used to match FK and
	// PK values this might be used for polymorphic relations
	polymorphicExpressions []string
}

// NewTableLink - creates a new TableLink instance.
func NewTableLink(
	id int,
	table commonmodels.Table,
	keys []Key,
	polymorphicExpressions []string,
) TableLink {
	return TableLink{
		ID:                     id,
		table:                  table,
		keys:                   keys,
		polymorphicExpressions: polymorphicExpressions,
	}
}

// TableID - returns the index of the table in the Graph.
func (tl TableLink) TableID() int {
	return tl.ID
}

// Table - returns the table itself.
func (tl TableLink) Table() commonmodels.Table {
	return tl.table
}

func (tl TableLink) FullTableName() string {
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

func (tl TableLink) DebugString() string {
	return fmt.Sprintf("TableLink[%d,%s (%v)]", tl.ID, tl.FullTableName(), tl.keys)
}
