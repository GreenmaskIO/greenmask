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
		Name:     "id2",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      1,
		NotNull:  false,
		Length:   2,
	},
	{
		Name:     "id4",
		TypeName: "int2",
		TypeOid:  pgtype.Int4OID,
		Num:      2,
		NotNull:  false,
		Length:   4,
	},
	{
		Name:     "id8",
		TypeName: "int2",
		TypeOid:  pgtype.Int8OID,
		Num:      3,
		NotNull:  false,
		Length:   8,
	},
	{
		Name:     "int2_val",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      4,
		NotNull:  false,
		Length:   2,
	},
	{
		Name:     "int4_val",
		TypeName: "int4",
		TypeOid:  pgtype.Int4OID,
		Num:      5,
		NotNull:  false,
		Length:   4,
	},
	{
		Name:     "int8_val",
		TypeName: "int8",
		TypeOid:  pgtype.Int8OID,
		Num:      6,
		NotNull:  false,
		Length:   8,
	},
	{
		Name:     "created_at",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      7,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "data",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      8,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "doc",
		TypeName: "jsonb",
		TypeOid:  pgtype.JSONBOID,
		Num:      9,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "uid",
		TypeName: "uuid",
		TypeOid:  pgtype.UUIDOID,
		Num:      10,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id2",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      11,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id4",
		TypeName: "int4",
		TypeOid:  pgtype.Int4OID,
		Num:      12,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id8",
		TypeName: "int8",
		TypeOid:  pgtype.Int8OID,
		Num:      13,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_date",
		TypeName: "date",
		TypeOid:  pgtype.DateOID,
		Num:      14,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_ts",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      15,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "date_tstz",
		TypeName: "timestamptz",
		TypeOid:  pgtype.TimestamptzOID,
		Num:      16,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_float4",
		TypeName: "float4",
		TypeOid:  pgtype.Float4OID,
		Num:      17,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_float8",
		TypeName: "float8",
		TypeOid:  pgtype.Float8OID,
		Num:      18,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "col_bool",
		TypeName: "bool",
		TypeOid:  pgtype.BoolOID,
		Num:      19,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id_numeric",
		TypeName: "numeric",
		TypeOid:  pgtype.NumericOID,
		Num:      20,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "val_numeric",
		TypeName: "numeric",
		TypeOid:  pgtype.NumericOID,
		Num:      21,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "data2",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      22,
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
