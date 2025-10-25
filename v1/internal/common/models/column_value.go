package models

import "fmt"

type ColumnValue struct {
	Value  any
	IsNull bool
}

func NewColumnValue(v any, isNull bool) *ColumnValue {
	return &ColumnValue{
		Value:  v,
		IsNull: isNull,
	}
}

func (s *ColumnValue) String() string {
	if s.IsNull {
		return "NULL"
	}
	return fmt.Sprintf("%v", s.Value)
}
