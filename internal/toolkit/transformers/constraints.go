package transformers

import (
	"golang.org/x/exp/slices"
)

type Constraint interface {
	IsAffected(table *Table, column *Column) bool
}

type DefaultConstraintDefinition struct {
	ConstraintSchema   string   `json:"constraintSchema"`
	ConstraintName     string   `json:"constraintName"`
	ConstraintOid      Oid      `json:"constraintOid"`
	ConstrainedColumns []AttNum `json:"constrainedColumns,omitempty"`
	TableSchema        string   `json:"tableSchema,omitempty"`
	TableName          string   `json:"tableName,omitempty"`
	TableOid           Oid      `json:"tableOid,omitempty"`
	Definition         string   `json:"definition,omitempty"`
	//RootPtConstraint   transformers.Oid      `json:"rootPtConstraint,omitempty"`
}

func (dcd *DefaultConstraintDefinition) IsAffected(table *Table, column *Column) bool {
	if table.Oid == dcd.TableOid {
		return slices.Contains(dcd.ConstrainedColumns, column.Num)
	}
	return false
}

type Check DefaultConstraintDefinition

func (c *Check) IsAffected(table *Table, column *Column) bool {
	if table.Oid == c.TableOid {
		return slices.Contains(c.ConstrainedColumns, column.Num)
	}
	return false
}

type Exclusion DefaultConstraintDefinition

func (e *Exclusion) IsAffected(table *Table, column *Column) bool {
	if table.Oid == e.TableOid {
		return slices.Contains(e.ConstrainedColumns, column.Num)
	}
	return false
}

type ForeignKey DefaultConstraintDefinition

func (fk *ForeignKey) IsAffected(table *Table, column *Column) bool {
	if table.Oid == fk.TableOid {
		return slices.Contains(fk.ConstrainedColumns, column.Num)
	}
	return false
}

type PrimaryKey DefaultConstraintDefinition

func (pk *PrimaryKey) IsAffected(table *Table, column *Column) bool {
	if table.Oid == pk.TableOid {
		return slices.Contains(pk.ConstrainedColumns, column.Num)
	}
	return false
}

type PrimaryKeyReferences DefaultConstraintDefinition

func (pkr *PrimaryKeyReferences) IsAffected(table *Table, column *Column) bool {
	if table.Oid == pkr.TableOid {
		return slices.Contains(pkr.ConstrainedColumns, column.Num)
	}
	return false
}

type Unique DefaultConstraintDefinition

func (u *Unique) IsAffected(table *Table, column *Column) bool {
	if table.Oid == u.TableOid {
		return slices.Contains(u.ConstrainedColumns, column.Num)
	}
	return false
}

type TriggerConstraint DefaultConstraintDefinition

func (tc *TriggerConstraint) IsAffected(table *Table, column *Column) bool {
	if table.Oid == tc.TableOid {
		return slices.Contains(tc.ConstrainedColumns, column.Num)
	}
	return false
}

type DomainConstrained struct {
	TableSchema      string
	TableName        string
	TableOid         Oid
	DomainOid        Oid
	DomainSchema     string
	DomainName       string
	BaseTypeOid      Oid
	BaseTypeName     string
	ConstraintOid    Oid
	ConstraintSchema string
	ConstraintName   string
	Definition       string
}

func (dc *DomainConstrained) IsAffected(table *Table, column *Column) bool {
	if table.Oid == dc.TableOid && column.TypeOid == dc.DomainOid {
		return true
	}
	return false
}

type DomainNotNullable struct {
	TableSchema    string
	TableName      string
	TableOid       Oid
	DomainOid      Oid
	DomainSchema   string
	DomainName     string
	DomainNullable bool
	BaseTypeOid    Oid
	BaseTypeName   string
}
