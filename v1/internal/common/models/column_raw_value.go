package models

type ColumnRawValue struct {
	Data   []byte `json:"d"`
	IsNull bool   `json:"n"`
}

func NewColumnRawValue(data []byte, isNull bool) *ColumnRawValue {
	return &ColumnRawValue{
		Data:   data,
		IsNull: isNull,
	}
}
