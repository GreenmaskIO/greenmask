package models

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
