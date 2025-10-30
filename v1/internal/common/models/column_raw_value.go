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

func (s *ColumnRawValue) String() string {
	if s.IsNull {
		return "NULL"
	}
	return string(s.Data)
}
