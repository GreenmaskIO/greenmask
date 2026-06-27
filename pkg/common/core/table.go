// Copyright 2025 Greenmask
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

package core

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
	Constraints    []Constraint `json:"-"`
	NeedDumpSchema bool         `json:"need_dump_schema"`
	NeedDumpData   bool         `json:"need_dump_data"`
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

// TypeID - engine-agnostic identifier of a database type. In PostgreSQL it maps
// to the type OID; other engines mint their own stable uint32 identifiers for
// their types. The uint32 underlying type keeps type lookups allocation-free.
type TypeID uint32

type TypeClass string

// TypeClass is an engine-agnostic family of a database type. core defines only
// the generic set shared by every engine plus two escape hatches. Engine drivers
// either map their native types onto this generic set or mint their own extension
// classes in the engine package (e.g. the MySQL driver's "enum" class). Consumers
// such as transformers must tolerate a class they do not recognize rather than
// switch exhaustively over this list.
const (
	// TypeClassUnsupported marks a type the engine could not classify.
	TypeClassUnsupported TypeClass = "unsupported"
	// TypeClassOther is the escape hatch for a type that is valid but does not
	// fit any generic class (e.g. an engine-specific family with no generic peer).
	TypeClassOther TypeClass = "other"

	TypeClassBinary   TypeClass = "binary"
	TypeClassText     TypeClass = "text"
	TypeClassInt      TypeClass = "int"
	TypeClassFloat    TypeClass = "float"
	TypeClassNumeric  TypeClass = "numeric"
	TypeClassBoolean  TypeClass = "boolean"
	TypeClassDateTime TypeClass = "datetime"
	TypeClassTime     TypeClass = "time"
	TypeClassJson     TypeClass = "json"
	TypeClassUuid     TypeClass = "uuid"
)

type Column struct {
	// Idx - column number in the table. It preserves the order of columns in the defined table.
	Idx int `json:"idx"`
	// Name - name of the column.
	Name string `json:"name"`
	// NotNull - indicates whether the column is NOT NULL.
	NotNull bool `json:"not_null"`
	// Pos
	Pos int `json:"pos"`
	// Type - the canonical, engine-agnostic descriptor of the column's data type.
	// All type metadata (name, id, class, signedness, precision/scale, ...) lives
	// here. It is built once before the column exists (engine projection on
	// introspection, JSON deserialization on restore, or a literal in tests) and
	// is never re-assembled afterward.
	Type Type `json:"type"`
}

func NewColumn(
	idx int,
	name string,
	notNull bool,
	t Type,
) Column {
	return Column{
		Idx:     idx,
		Name:    name,
		NotNull: notNull,
		Type:    t,
	}
}
