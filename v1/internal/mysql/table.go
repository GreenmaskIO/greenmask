package mysql

type Table struct {
	Schema     string
	Name       string
	Columns    []Column
	Size       int64
	PrimaryKey []string
}

func NewTable(schema, name string, size int64) Table {
	return Table{
		Schema: schema,
		Name:   name,
		Size:   size,
	}
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
