package toolkit

var DefaultNullSeq RawRecordText = []byte("\\N")

type RawRecordText []byte

func NewRawRecordText() *RawRecordText {
	return new(RawRecordText)
}

func (r *RawRecordText) GetColumn(idx int) (*RawValue, error) {
	if string(*r) == string(DefaultNullSeq) {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue(*r, false), nil
}

func (r *RawRecordText) SetColumn(idx int, v *RawValue) error {
	if v.IsNull {
		*r = DefaultNullSeq
	}
	*r = v.Data
	return nil
}

func (r *RawRecordText) Encode() ([]byte, error) {
	return *r, nil
}

func (r *RawRecordText) Decode(data []byte) error {
	*r = data
	return nil
}

func (r *RawRecordText) Length() int {
	return 1
}
