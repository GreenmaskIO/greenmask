package validate_utils

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type testTransformer struct{}

func (tt *testTransformer) Init(ctx context.Context) error {
	return nil
}

func (tt *testTransformer) Done(ctx context.Context) error {
	return nil
}

func (tt *testTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	return nil, nil
}

func (tt *testTransformer) GetAffectedColumns() map[int]string {
	return map[int]string{
		1: "name",
	}
}

func TestJsonDocument_GetAffectedColumns(t *testing.T) {
	tab, _, _ := getTableAndRows()
	jd := NewJsonDocument(tab, true, true)
	colsToPrint := jd.GetColumnsToPrint()
	_, ok := colsToPrint["departmentid"]
	assert.True(t, ok)
	_, ok = colsToPrint["name"]
	assert.True(t, ok)
}

func TestJsonDocument_GetRecords(t *testing.T) {
	tab, originalRecs, transformedRecs := getTableAndRows()
	log.Warn().Any("1", t)
	log.Warn().Any("1", originalRecs)
	log.Warn().Any("1", transformedRecs)

	original := pgcopy.NewRow(4)
	transformed := pgcopy.NewRow(4)

	jd := NewJsonDocument(tab, true, true)

	for idx := range originalRecs {
		err := original.Decode(originalRecs[idx])
		require.NoErrorf(t, err, "error at %d line", idx)
		err = transformed.Decode(transformedRecs[idx])
		require.NoErrorf(t, err, "error at %d line", idx)
		err = jd.Append(original, transformed)
		require.NoErrorf(t, err, "error at %d line", idx)
	}
	columnsToPrint := jd.GetColumnsToPrint()
	assert.Equal(t, columnsToPrint, map[string]struct{}{"departmentid": {}, "name": {}, "modifieddate": {}})

	columnsImplicitlyChanged := jd.GetUnexpectedlyChangedColumns()
	assert.Equal(t, columnsImplicitlyChanged, map[string]struct{}{"modifieddate": {}})

	result := jd.Get()
	require.Len(t, result.RecordsWithDiff, 6)

	//log.Warn().Any("a", records)

	//driver, err := toolkit.NewDriver(table, nil, nil)
	//if err != nil {
	//	panic(err.Error())
	//}
	//row := pgcopy.NewRow(1)
	//_ = row.Decode([]byte(value))
	//r := toolkit.NewRecord(
	//	driver,
	//)
	//r.SetRow(row)
}

func getTableAndRows() (table *entries.Table, original, transformed [][]byte) {

	tableDef := `
		{
		  "schema": "humanresources",
		  "name": "department",
		  "oid": 16526,
		  "columns": [
			{
			  "name": "departmentid",
			  "type_name": "integer",
			  "type_oid": 23,
			  "num": 1,
			  "not_null": true,
			  "length": -1
			},
			{
			  "name": "name",
			  "type_name": "\"Name\"",
			  "type_oid": 16426,
			  "num": 2,
			  "not_null": true,
			  "length": -1
			},
			{
			  "name": "groupname",
			  "type_name": "\"Name\"",
			  "type_oid": 16426,
			  "num": 3,
			  "not_null": true,
			  "length": -1
			},
			{
			  "name": "modifieddate",
			  "type_name": "timestamp without time zone",
			  "type_oid": 1114,
			  "num": 4,
			  "not_null": true,
			  "length": -1
			}
		  ]
		}
	`

	colnstraintDef := `
		{
		  "schema": "humanresources",
		  "name": "PK_Department_DepartmentID",
		  "oid": 17387,
		  "columns": [
			1
		  ],
		  "definition": "PRIMARY KEY (departmentid)"
		}
	`

	original = [][]byte{
		[]byte("1\tEngineering\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("2\tTool Design\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("3\tSales\tSales and Marketing\t2008-04-30 00:00:00"),
		[]byte("4\tMarketing\tSales and Marketing\t2008-04-30 00:00:00"),
		[]byte("5\tPurchasing\tInventory Management\t2008-04-30 00:00:00"),
		[]byte("6\tResearch and Development\tResearch and Development\t2008-04-30 00:00:00"),
	}

	transformed = [][]byte{
		[]byte("1\ttes1\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("2\ttes2\tResearch and Development\t2008-04-30 00:00:00"),
		[]byte("3\ttes3\tSales and Marketing\t2008-04-30 00:00:00"),
		[]byte("4\ttes4\tSales and Marketing\t\\N"),
		[]byte("5\ttes5\tInventory Management\t2008-04-30 00:00:00"),
		[]byte("6\ttes6 and Development\tResearch and Development\t2008-04-30 00:00:00"),
	}

	c := &toolkit.PrimaryKey{}
	err := json.Unmarshal([]byte(colnstraintDef), c)
	if err != nil {
		panic(err)
	}

	t := &toolkit.Table{
		Constraints: []toolkit.Constraint{c},
	}
	err = json.Unmarshal([]byte(tableDef), t)
	if err != nil {
		panic(err)
	}

	table = &entries.Table{
		Table: t,
		TransformersContext: []*utils.TransformerContext{
			{Transformer: &testTransformer{}},
		},
	}

	return table, original, transformed
}
