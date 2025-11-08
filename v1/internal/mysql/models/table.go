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

package models

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Table struct {
	ID         int
	Schema     string
	Name       string
	Columns    []Column
	Size       *int64
	PrimaryKey []string
	References []models.Reference
}

func NewTable(id int, schema, name string, size *int64) Table {
	return Table{
		ID:     id,
		Schema: schema,
		Name:   name,
		Size:   size,
	}
}

func (t *Table) ToCommonTable() models.Table {
	columns := make([]models.Column, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = models.NewColumn(
			i,
			col.Name,
			col.TypeName,
			col.TypeOID,
			col.NotNull,
		)
	}
	return models.Table{
		ID:         t.ID,
		Schema:     t.Schema,
		Name:       t.Name,
		PrimaryKey: t.PrimaryKey,
		References: t.References,
		Columns:    columns,
	}
}

func (t *Table) SetColumns(columns []Column) {
	t.Columns = columns
}

func (t *Table) SetPrimaryKey(pk []string) {
	t.PrimaryKey = pk
}

func (t *Table) SetReferences(refs []models.Reference) {
	t.References = refs
}
