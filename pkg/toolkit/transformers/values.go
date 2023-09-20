package transformers

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
