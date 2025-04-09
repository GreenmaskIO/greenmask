package models

import "fmt"

type Constraint interface {
	// Columns - returns the list of columns that are affected by the constraint.
	Columns() []string
	// Type - returns the type of the constraint.
	Type() string
	// Name - returns the name of the constraint.
	Name() string
	// Definition - returns transformer definition in the database.
	Definition() string
}

type Table struct {
	// ID - runtime identifier of the table.
	// It's important to fill ID of a table in runtime. And this ID is an index of table
	// in the table slice.
	ID int
	// Schema - schema name of the table.
	Schema string
	// Name - name of the table.
	Name    string
	Columns []Column
	// Size - size of the table in bytes.
	Size int64
	// PrimaryKey - list of primary key column names.
	PrimaryKey []string
	// References - list of references to other tables.
	References []Reference
	// SubsetConditions - list of conditions that are used to filter the table data.
	SubsetConditions []string
	// Constraints - list of constraints that are defined on the table.
	Constraints []Constraint
}

// FullTableName - returns the full table name.
func (t Table) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

func (t Table) DebugString() string {
	return fmt.Sprintf(
		"Table[schema=%s name=%s]",
		t.Schema,
		t.Name,
	)
}

func (t Table) HasSubsetConditions() bool {
	return len(t.SubsetConditions) > 0
}

// VirtualOID - represents OID in PostgreSQL, but at the same time might be used
// for any other DB by the uint32 mapping to the real type. Don't know,
// maybe we should rename it to the ObjectID or smth like that.
// This is expected to reduce allocations when accessing to the types or
// any other database/table objects.
type VirtualOID uint32

type Column struct {
	// Idx - column number in the table. It preserves the order of columns in the defined table.
	Idx int
	// Name - name of the column.
	Name string
	// TypeName - name of the column type, e.g. "integer", "text", "boolean", etc.
	TypeName string
	// NotNull - indicates whether the column is NOT NULL.
	NotNull bool
	// TypeOID - OID of the column type in PostgreSQL. For other DBMS that does not have OIDs,
	// this is just a unique identifier of the type in the greenmask implementation.
	TypeOID VirtualOID
	// Name - column number in the table. It is used to identify the column in the tuple.
	Length int
}

func NewColumn(idx int, name string, colTyp string, oid VirtualOID, notNull bool) Column {
	return Column{
		Idx:      idx,
		Name:     name,
		TypeName: colTyp,
		TypeOID:  oid,
		NotNull:  notNull,
	}
}
