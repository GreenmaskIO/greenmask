package introspect

import (
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Table struct {
	Schema     string
	Name       string
	Columns    []Column
	Size       *int64
	PrimaryKey []string
	References []models.Reference
}

func NewTable(schema, name string, size *int64) Table {
	return Table{
		Schema: schema,
		Name:   name,
		Size:   size,
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

type Column struct {
	Idx               int
	Name              string
	TypeName          string
	DataType          *string
	NumericPrecision  *int
	NumericScale      *int
	DateTimePrecision *int
	NotNull           bool
}

func NewColumn(
	idx int,
	name, typeName string,
	dataType *string,
	numericPrecision, numericScale, dateTimePrecision *int,
	notNull bool,
) Column {
	return Column{
		Idx:               idx,
		Name:              name,
		TypeName:          typeName,
		DataType:          dataType,
		NumericPrecision:  numericPrecision,
		NumericScale:      numericScale,
		DateTimePrecision: dateTimePrecision,
		NotNull:           notNull,
	}
}
