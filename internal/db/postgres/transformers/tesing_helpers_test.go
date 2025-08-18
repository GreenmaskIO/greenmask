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

package transformers

import (
	"slices"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var columnList = []*toolkit.Column{
	{
		Name:       "id",
		TypeName:   "int2",
		TypeOid:    pgtype.Int2OID,
		Num:        1,
		NotNull:    false,
		Length:     -1,
		TypeLength: 2,
	},
	{
		Name:       "created_at",
		TypeName:   "timestamp",
		TypeOid:    pgtype.TimestampOID,
		Num:        2,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
	},
	{
		Name:       "data",
		TypeName:   "text",
		TypeOid:    pgtype.TextOID,
		Num:        3,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
	},
	{
		Name:       "doc",
		TypeName:   "jsonb",
		TypeOid:    pgtype.JSONBOID,
		Num:        4,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
	},
	{
		Name:       "uid",
		TypeName:   "uuid",
		TypeOid:    pgtype.UUIDOID,
		Num:        5,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
	},
	{
		Name:       "id2",
		TypeName:   "int2",
		TypeOid:    pgtype.Int2OID,
		Num:        6,
		NotNull:    false,
		TypeLength: 2,
		Length:     -1,
	},
	{
		Name:       "id4",
		TypeName:   "int4",
		TypeOid:    pgtype.Int4OID,
		Num:        7,
		NotNull:    false,
		Length:     -1,
		TypeLength: 4,
	},
	{
		Name:       "id8",
		TypeName:   "int8",
		TypeOid:    pgtype.Int8OID,
		Num:        8,
		NotNull:    false,
		TypeLength: 8,
		Length:     -1,
	},
	{
		Name:       "date_date",
		TypeName:   "date",
		TypeOid:    pgtype.DateOID,
		Num:        9,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
	},
	{
		Name:       "date_ts",
		TypeName:   "timestamp",
		TypeOid:    pgtype.TimestampOID,
		Num:        10,
		NotNull:    false,
		Length:     -1,
		TypeLength: -1,
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
		Name:       "col_float4",
		TypeName:   "float4",
		TypeOid:    pgtype.Float4OID,
		Num:        12,
		NotNull:    false,
		Length:     -1,
		TypeLength: 4,
	},
	{
		Name:       "col_float8",
		TypeName:   "float8",
		TypeOid:    pgtype.Float8OID,
		Num:        13,
		NotNull:    false,
		Length:     -1,
		TypeLength: 8,
	},
	{
		Name:     "col_bool",
		TypeName: "bool",
		TypeOid:  pgtype.BoolOID,
		Num:      14,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "id_numeric",
		TypeName: "numeric",
		TypeOid:  pgtype.NumericOID,
		Num:      15,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "val_numeric",
		TypeName: "numeric",
		TypeOid:  pgtype.NumericOID,
		Num:      16,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "macaddress",
		TypeName: "numeric",
		TypeOid:  pgtype.MacaddrOID,
		Num:      17,
		NotNull:  false,
		Length:   -1,
	},
	{
		Name:     "first_name",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      18,
		NotNull:  false,
		Length:   -1,
	},

	{
		Name:     "last_name",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      19,
		NotNull:  false,
		Length:   -1,
	},
}

// getDriverAndRecord - return adhoc table for testing
// TODO: You should generate table definition dynamically using faker as well as table tuples
func getDriverAndRecord(name string, value string) (*toolkit.Driver, *toolkit.Record) {
	idx := slices.IndexFunc(columnList, func(column *toolkit.Column) bool {
		return column.Name == name
	})

	if idx == -1 {
		panic("cannot find column")
	}

	table := &toolkit.Table{
		Schema:      "public",
		Name:        "test",
		Oid:         1224,
		Columns:     columnList[idx : idx+1],
		Constraints: []toolkit.Constraint{},
	}

	driver, warns, err := toolkit.NewDriver(table, nil)
	if len(warns) > 0 {
		panic("warn")
	}
	if err != nil {
		panic(err.Error())
	}
	row := pgcopy.NewRow(1)
	_ = row.Decode([]byte(value))
	r := toolkit.NewRecord(
		driver,
	)
	r.SetRow(row)
	return driver, r
}

func getDriverAndRecordByColumns(columnNames []string, value string) (*toolkit.Driver, *toolkit.Record) {
	filteredColumns := make([]*toolkit.Column, 0, len(columnList))
	for _, columnName := range columnNames {
		idx := slices.IndexFunc(columnList, func(column *toolkit.Column) bool {
			return column.Name == columnName
		})
		if idx == -1 {
			panic("cannot find column: " + columnName)
		}
		filteredColumns = append(filteredColumns, columnList[idx])
	}

	table := &toolkit.Table{
		Schema:      "public",
		Name:        "test",
		Oid:         1224,
		Columns:     filteredColumns,
		Constraints: []toolkit.Constraint{},
	}

	driver, warns, err := toolkit.NewDriver(table, nil)
	if len(warns) > 0 {
		panic("warn")
	}
	if err != nil {
		panic(err.Error())
	}
	row := pgcopy.NewRow(len(columnList))
	_ = row.Decode([]byte(value))
	r := toolkit.NewRecord(
		driver,
	)
	r.SetRow(row)
	return driver, r
}
