package toolkit

type RawValue struct {
	Data   []byte
	IsNull bool
}

func NewRawValue(data []byte, isNull bool) *RawValue {
	return &RawValue{
		Data:   data,
		IsNull: isNull,
	}
}

type Value struct {
	Value  any
	IsNull bool
}

func NewValue(v any, isNull bool) *Value {
	return &Value{
		Value:  v,
		IsNull: isNull,
	}
}

type RawValueDto struct {
	Data   string `json:"d"`
	IsNull bool   `json:"n"`
}

func NewRawValueDto(data string, isNull bool) *RawValueDto {
	return &RawValueDto{
		Data:   data,
		IsNull: isNull,
	}
}
