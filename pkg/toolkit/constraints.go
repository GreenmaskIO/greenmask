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
	"slices"
)

const (
	FkConstraintType           = "ForeignKey"
	CheckConstraintType        = "Check"
	NotNullConstraintType      = "NotNull"
	PkConstraintType           = "PrimaryKey"
	PkConstraintReferencesType = "PrimaryKeyReferences"
	UniqueConstraintType       = "Unique"
	LengthConstraintType       = "Length"
	ExclusionConstraintType    = "Exclusion"
	TriggerConstraintType      = "TriggerConstraint"
)

type Constraint interface {
	IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings)
}

type DefaultConstraintDefinition struct {
	// Schema - constraint schema name
	Schema string `json:"schema"`
	// Name - constraint name
	Name string `json:"name"`
	// Oid - Constraint oid in pg_constraint
	Oid Oid `json:"oid"`
	// Columns - columns involved into constraint
	Columns []AttNum `json:"columns,omitempty"`
	// Definition - real textual constraint definition
	Definition string `json:"definition,omitempty"`
}

type Check DefaultConstraintDefinition

func NewCheck(schema, name, definition string, oid Oid, columns []AttNum) *Check {
	return &Check{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
}

func (c *Check) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(c.Columns, column.Num) {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ColumnName", column.Name).
			AddMeta("ConstraintType", CheckConstraintType).
			AddMeta("ConstraintSchema", c.Schema).
			AddMeta("ConstraintName", c.Schema).
			AddMeta("ConstraintDef", c.Definition).
			SetMsgf("possible constraint violation: column has %s constraint", CheckConstraintType),
		)
	}
	return
}

type Exclusion DefaultConstraintDefinition

func NewExclusion(schema, name, definition string, oid Oid, columns []AttNum) *Exclusion {
	return &Exclusion{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
}

func (e *Exclusion) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(e.Columns, column.Num) {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ColumnName", column.Name).
			AddMeta("ConstraintType", ExclusionConstraintType).
			AddMeta("ConstraintSchema", e.Schema).
			AddMeta("ConstraintName", e.Schema).
			AddMeta("ConstraintDef", e.Definition).
			SetMsgf("possible constraint violation: column is involved into %s constraint", ExclusionConstraintType),
		)
	}
	return w
}

// LinkedTable - table that involved into constraint, required for ForeignKey and PrimaryKeyReferences
type LinkedTable struct {
	// Schema - table schema name
	Schema string `json:"schema"`
	// Name - table name
	Name string `json:"name"`
	// Oid - table oid
	Oid Oid `json:"oid"`
	// Constraint - linked table constraint
	Constraint Constraint `json:"constraint,omitempty"`
}

type ForeignKey struct {
	DefaultConstraintDefinition
	// ReferencedTable - table that has primary key definition on that discovering table is referencing
	ReferencedTable LinkedTable `json:"referencedTable,omitempty"`
}

func NewForeignKey(schema, name, definition string, oid Oid, columns []AttNum, referencedTable LinkedTable) *ForeignKey {
	return &ForeignKey{
		DefaultConstraintDefinition: DefaultConstraintDefinition{
			Schema:     schema,
			Name:       name,
			Oid:        oid,
			Columns:    columns,
			Definition: definition,
		},
		ReferencedTable: referencedTable,
	}
}

func (fk *ForeignKey) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(fk.Columns, column.Num) {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ColumnName", column.Name).
			AddMeta("ConstraintType", FkConstraintType).
			AddMeta("ConstraintSchema", fk.Schema).
			AddMeta("ConstraintName", fk.Schema).
			AddMeta("ConstraintDef", fk.Definition).
			SetMsgf("possible constraint violation: column is involved into %s constraint", FkConstraintType),
		)
	}
	return w
}

type PrimaryKey struct {
	DefaultConstraintDefinition
	References []*LinkedTable
}

func NewPrimaryKey(schema, name, definition string, oid Oid, columns []AttNum) *PrimaryKey {
	return &PrimaryKey{
		DefaultConstraintDefinition: DefaultConstraintDefinition{
			Schema:     schema,
			Name:       name,
			Oid:        oid,
			Columns:    columns,
			Definition: definition,
		},
	}
}

func (pk *PrimaryKey) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(pk.Columns, column.Num) {
		if columnProperties != nil && !columnProperties.Unique || columnProperties == nil {
			w = append(w, NewValidationWarning().
				SetSeverity(WarningValidationSeverity).
				AddMeta("ColumnName", column.Name).
				AddMeta("ConstraintType", PkConstraintType).
				AddMeta("ConstraintSchema", pk.Schema).
				AddMeta("ConstraintName", pk.Schema).
				AddMeta("ConstraintDef", pk.Definition).
				SetMsgf("possible constraint violation: column is involved into %s constraint", PkConstraintType),
			)
		}

		for _, ref := range pk.References {
			w = append(w, NewValidationWarning().
				SetSeverity(WarningValidationSeverity).
				AddMeta("ColumnName", column.Name).
				AddMeta("ConstraintType", PkConstraintReferencesType).
				AddMeta("ConstraintSchema", pk.Schema).
				AddMeta("ConstraintName", pk.Schema).
				AddMeta("ConstraintDef", pk.Definition).
				AddMeta("ReferencedTable", ref).
				SetMsgf("possible constraint violation: column is primary key and has references"),
			)
		}
	}

	return w
}

type Unique DefaultConstraintDefinition

func NewUnique(schema, name, definition string, oid Oid, columns []AttNum) *Unique {
	return &Unique{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
}

func (u *Unique) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(u.Columns, column.Num) && (columnProperties != nil && !columnProperties.Unique || columnProperties == nil) {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ColumnName", column.Name).
			AddMeta("ConstraintType", UniqueConstraintType).
			AddMeta("ConstraintSchema", u.Schema).
			AddMeta("ConstraintName", u.Schema).
			AddMeta("ConstraintDef", u.Definition).
			SetMsgf("possible constraint violation: column is involved into %s constraint", UniqueConstraintType),
		)
	}
	return w
}

type TriggerConstraint DefaultConstraintDefinition

func NewTriggerConstraint(schema, name, definition string, oid Oid, columns []AttNum) *TriggerConstraint {
	return &TriggerConstraint{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
}

func (tc *TriggerConstraint) IsAffected(column *Column, columnProperties *ColumnProperties) (w ValidationWarnings) {
	if slices.Contains(tc.Columns, column.Num) {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ColumnName", column.Name).
			AddMeta("ConstraintType", TriggerConstraintType).
			AddMeta("ConstraintSchema", tc.Schema).
			AddMeta("ConstraintName", tc.Schema).
			AddMeta("ConstraintDef", tc.Definition).
			SetMsgf("possible constraint violation: column is involved into %s constraint", TriggerConstraintType),
		)
	}
	return w
}
