package models

type VirtualOID uint32

type Column struct {
	Idx     int
	Name    string
	Type    string
	TypeOID VirtualOID
}

func NewColumn(name, typ string, oid VirtualOID) Column {
	return Column{
		Name:    name,
		Type:    typ,
		TypeOID: oid,
	}
}

type Table struct {
	Schema  string
	Name    string
	Columns []Column
}

func NewTable(schema, name string, columns []Column) Table {
	return Table{
		Schema:  schema,
		Name:    name,
		Columns: columns,
	}
}
