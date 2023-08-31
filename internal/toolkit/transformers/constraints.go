package transformers

import (
	"golang.org/x/exp/slices"
)

type Constraint interface {
	IsAffected(column *Column) bool
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

func (c *Check) IsAffected(column *Column) bool {
	return slices.Contains(c.Columns, column.Num)
}

func NewCheck(schema, name, definition string, oid Oid, columns []AttNum) *Check {
	return &Check{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
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

func (e *Exclusion) IsAffected(column *Column) bool {
	return slices.Contains(e.Columns, column.Num)
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
	Constraint Constraint
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

func (fk *ForeignKey) IsAffected(column *Column) bool {
	return slices.Contains(fk.Columns, column.Num)
}

type PrimaryKey DefaultConstraintDefinition

func NewPrimaryKey(schema, name, definition string, oid Oid, columns []AttNum) *PrimaryKey {
	return &PrimaryKey{
		Schema:     schema,
		Name:       name,
		Oid:        oid,
		Columns:    columns,
		Definition: definition,
	}
}

func (pk *PrimaryKey) IsAffected(column *Column) bool {
	return slices.Contains(pk.Columns, column.Num)
}

type PrimaryKeyReferences struct {
	DefaultConstraintDefinition
	// OnTable - table that has foreign key reference on the discovering table primary key
	OnTable LinkedTable `json:"onTable,omitempty"`
}

func NewPrimaryKeyReferences(schema, name, definition string, oid Oid, columns []AttNum, onTable LinkedTable) *PrimaryKeyReferences {
	return &PrimaryKeyReferences{
		DefaultConstraintDefinition: DefaultConstraintDefinition{
			Schema:     schema,
			Name:       name,
			Oid:        oid,
			Columns:    columns,
			Definition: definition,
		},
		OnTable: onTable,
	}
}

func (pkr *PrimaryKeyReferences) IsAffected(column *Column) bool {
	return slices.Contains(pkr.Columns, column.Num)
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

func (u *Unique) IsAffected(column *Column) bool {
	return slices.Contains(u.Columns, column.Num)
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

func (tc *TriggerConstraint) IsAffected(column *Column) bool {
	return slices.Contains(tc.Columns, column.Num)
}
