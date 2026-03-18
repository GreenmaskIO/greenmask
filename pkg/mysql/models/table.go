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
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type Table struct {
	ID             int
	Schema         string
	Name           string
	Columns        []Column
	Size           *int64
	PrimaryKey     []string
	References     []commonmodels.Reference
	NeedDumpSchema bool
	NeedDumpData   bool
}

func (t *Table) ToCommonTable() commonmodels.Table {
	columns := make([]commonmodels.Column, len(t.Columns))
	for i, col := range t.Columns {
		columns[i] = commonmodels.NewColumn(
			i,
			col.Name,
			col.TypeName,
			col.TypeOID,
			col.NotNull,
			col.TypeClass,
		)
	}
	return commonmodels.Table{
		ID:             t.ID,
		Schema:         t.Schema,
		Name:           t.Name,
		PrimaryKey:     t.PrimaryKey,
		References:     t.References,
		Columns:        columns,
		NeedDumpSchema: t.NeedDumpSchema,
		NeedDumpData:   t.NeedDumpData,
	}
}

func (t *Table) SetColumns(columns []Column) {
	t.Columns = columns
}

func (t *Table) SetPrimaryKey(pk []string) {
	t.PrimaryKey = pk
}

func (t *Table) SetReferences(refs []commonmodels.Reference) {
	t.References = refs
}

func (t *Table) GetFullTableName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}
