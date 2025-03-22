package common

import (
	"fmt"

	"github.com/greenmaskio/greenmask/v1/internal/common/models"
)

type Table struct {
	Schema           string
	Name             string
	Columns          []*Column
	Size             int64
	PrimaryKey       []string
	References       []models.Reference
	SubsetConditions []string
}

// TableName - returns the full table name.
func (t Table) TableName() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Name)
}

func (t Table) DebugString() string {
	return fmt.Sprintf(
		"Table[schema=%s name=%s]",
		t.Schema,
		t.Name,
	)
}

type Column struct {
	Idx      int
	Name     string
	TypeName string
	// TypeOid - can be either a real oid like in postgresql or virtual oid that exists only in
	// the driver implementation
	TypeOid           uint32
	CanonicalTypeName string
	NotNull           bool
	Size              int
}

func (c Column) DebugString() string {
	return fmt.Sprintf(
		"Column[name=%s type=%s not_null=%t]",
		c.Name,
		c.TypeName,
		c.NotNull,
	)
}
