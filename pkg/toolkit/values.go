package toolkit

type RawValue struct {
	Data   []byte `json:"d"`
	IsNull bool   `json:"n"`
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
	Data   *string `json:"d"`
	IsNull bool    `json:"n"`
}

func NewRawValueDto(data []byte, isNull bool) *RawValueDto {
	res := string(data)
	return &RawValueDto{
		Data:   &res,
		IsNull: isNull,
	}
}
