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
//		keys = append(keys, &Key{ID: col.ID, Expression: col.Expression})
//	}
//	return keys
//}

func (k *Key) GetKeyReference(t commonmodels.Table) string {
	if k.Expression != "" {
		return k.Expression
	}
	return fmt.Sprintf(`"%s"."%s"."%s"`, t.Schema, t.Name, k.Name)
}
