// Copyright 2023 Greenmask
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

package toolkit

import (
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
)

var columnList = []*Column{
	{
		Name:     "id",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      1,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "int_val2",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      1,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "created_at",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      2,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "data",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      3,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "doc",
		TypeName: "jsonb",
		TypeOid:  pgtype.JSONBOID,
		Num:      4,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "uid",
		TypeName: "uuid",
		TypeOid:  pgtype.UUIDOID,
		Num:      5,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id2",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      6,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id4",
		TypeName: "int4",
		TypeOid:  pgtype.Int4OID,
		Num:      7,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id8",
		TypeName: "int8",
		TypeOid:  pgtype.Int8OID,
		Num:      8,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_date",
		TypeName: "date",
		TypeOid:  pgtype.DateOID,
		Num:      9,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_ts",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      10,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_tstz",
		TypeName: "timestamptz",
		TypeOid:  pgtype.TimestamptzOID,
		Num:      11,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_float4",
		TypeName: "float4",
		TypeOid:  pgtype.Float4OID,
		Num:      12,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_float8",
		TypeName: "float8",
		TypeOid:  pgtype.Float8OID,
		Num:      13,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_bool",
		TypeName: "bool",
		TypeOid:  pgtype.BoolOID,
		Num:      15,
		NotNull:  false,
		Length:   -1,
	},
}

// GetDriverAndRecord - return adhoc table for testing
// TODO: You should generate table definition dynamically using faker as well as table tuples
func GetDriverAndRecord(columnValues map[string]*RawValue) (*Driver, *Record) {

	if columnValues == nil || len(columnValues) == 0 {
		panic("received empty columnValues")
	}

	var columns []*Column
	rawRecord := make(RawRecord)
	var colNum int
	for columnName, columnValue := range columnValues {
		idx := slices.IndexFunc(columnList, func(column *Column) bool {
			return column.Name == columnName
		})
		if idx == -1 {
			panic(fmt.Sprintf("column with name \"%s\" is not found", columnName))
		}
		columns = append(columns, columnList[idx])
		rawRecord[colNum] = columnValue
		colNum++
	}

	table := &Table{
		Schema:      "public",
		Name:        "test",
		Oid:         1224,
		Columns:     columns,
		Constraints: []Constraint{},
	}

	driver, _, err := NewDriver(table, nil)
	if err != nil {
		panic(err.Error())
	}
	r := NewRecord(
		driver,
	)
	r.SetRow(&rawRecord)
	return driver, r
}