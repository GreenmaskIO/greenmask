package toolkit

var DefaultNullSeq RawRecordText = []byte("\\N")
var DefaultEscapedNullSeq RawRecordText = []byte("\\\\N")

type RawRecordText []byte

func NewRawRecordText() *RawRecordText {
	return new(RawRecordText)
}

func (r *RawRecordText) GetColumn(idx int) (*RawValue, error) {
	if r == &DefaultNullSeq {
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
	if len(*r) == 2 && (*r)[0] == '\\' && (*r)[2] == 'N' {
		return DefaultEscapedNullSeq, nil
	}
	return *r, nil
}

func (r *RawRecordText) Decode(data []byte) error {
	if len(data) == 3 && data[0] == '\\' && data[1] == '\\' && data[2] == 'N' {
		*r = DefaultNullSeq
	} else {
		*r = data
	}
	return nil
}

func (r *RawRecordText) Length() int {
	return 1
}

func (r *RawRecordText) Clean() {
	*r = (*r)[:0]
}
