// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
