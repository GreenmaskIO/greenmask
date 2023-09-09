package transformers

import (
	"slices"

	"github.com/jackc/pgx/v5/pgtype"

	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

var columnList = []*toolkit.Column{
	{
		Name:     "id",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      1,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "created_at",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      2,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "data",
		TypeName: "text",
		TypeOid:  pgtype.TextOID,
		Num:      3,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "doc",
		TypeName: "jsonb",
		TypeOid:  pgtype.JSONBOID,
		Num:      4,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "uid",
		TypeName: "uuid",
		TypeOid:  pgtype.UUIDOID,
		Num:      5,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "id2",
		TypeName: "int2",
		TypeOid:  pgtype.Int2OID,
		Num:      6,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "id4",
		TypeName: "int4",
		TypeOid:  pgtype.Int4OID,
		Num:      7,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "id8",
		TypeName: "int8",
		TypeOid:  pgtype.Int8OID,
		Num:      8,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "date_date",
		TypeName: "date",
		TypeOid:  pgtype.DateOID,
		Num:      9,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "date_ts",
		TypeName: "timestamp",
		TypeOid:  pgtype.TimestampOID,
		Num:      10,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "date_tstz",
		TypeName: "timestamptz",
		TypeOid:  pgtype.TimestamptzOID,
		Num:      11,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "col_float4",
		TypeName: "float4",
		TypeOid:  pgtype.Float4OID,
		Num:      12,
		NotNull:  true,
		Length:   -1,
	},
	{
		Name:     "col_float8",
		TypeName: "float8",
		TypeOid:  pgtype.Float8OID,
		Num:      13,
		NotNull:  true,
		Length:   -1,
	},
}

// getDriverAndRecord - return adhoc table for testing
// TODO: You should generate table definition it dynamically using faker as well as table tuples
func getDriverAndRecord(name string, value string) (*toolkit.Driver, *toolkit.Record) {
	typeMap := pgtype.NewMap()

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

	driver, err := toolkit.NewDriver(typeMap, table)
	if err != nil {
		panic(err.Error())
	}
	return driver, toolkit.NewRecord(
		driver,
		[]string{value},
	)
}
