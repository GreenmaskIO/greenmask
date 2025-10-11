package models

import (
	"errors"
	"fmt"
)

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
	ID int `json:"id"`
	// Schema - schema name of the table.
	Schema string `json:"schema"`
	// Name - name of the table.
	Name    string   `json:"name"`
	Columns []Column `json:"columns"`
	// Size - size of the table in bytes.
	Size int64 `json:"size"`
	// PrimaryKey - list of primary key column names.
	PrimaryKey []string `json:"primary_key"`
	// References - list of references to other tables.
	References []Reference `json:"-"`
	// SubsetConditions - list of conditions  that are used to filter the table data.
	SubsetConditions []string `json:"-"`
	// Constraints - list of constraints that are defined on the table.
	Constraints []Constraint `json:"-"`
}

var (
	errTableNameIsEmpty  = errors.New("table name is empty")
	errSchemaNameIsEmpty = errors.New("schema name is empty")
)

func (t *Table) Validate() error {
	if t.Name == "" {
		return errTableNameIsEmpty
	}
	if t.Schema == "" {
		return errSchemaNameIsEmpty
	}
	return nil
}

// FullTableName - returns the full table name.
func (t *Table) FullTableName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

func (t *Table) DebugString() string {
	return fmt.Sprintf(
		"Table[schema=%s name=%s]",
		t.Schema,
		t.Name,
	)
}

func (t *Table) HasSubsetConditions() bool {
	return len(t.SubsetConditions) > 0
}

// VirtualOID - represents OID in PostgreSQL, but at the same time might be used
// for any other DB by the uint32 mapping to the real type. Don't know,
// maybe we should rename it to the TaskID or smth like that.
// This is expected to reduce allocations when accessing to the types or
// any other database/table objects.
type VirtualOID uint32

type Column struct {
	// Idx - column number in the table. It preserves the order of columns in the defined table.
	Idx int `json:"idx"`
	// Name - name of the column.
	Name string `json:"name"`
	// TypeName - name of the column type, e.g. "integer", "text", "boolean", etc.
	TypeName string `json:"type_name"`
	// NotNull - indicates whether the column is NOT NULL.
	NotNull bool `json:"not_null"`
	// TypeOID - OID of the column type in PostgreSQL. For other DBMS that does not have OIDs,
	// this is just a unique identifier of the type in the greenmask implementation.
	TypeOID VirtualOID `json:"type_oid"`
	// Length - length of the column type, e.g. for varchar(255) it will be 255.
	Length int `json:"length"`
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
