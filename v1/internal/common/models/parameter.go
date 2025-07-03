package models

type ParamsValue []byte

func NewParamsValue(value []byte) ParamsValue {
	v := make(ParamsValue, len(value))
	copy(v, value)
	return v
}
