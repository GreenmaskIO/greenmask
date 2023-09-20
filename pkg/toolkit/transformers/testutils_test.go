package transformers

var testNullSeq = "\\N"
var testDelim byte = '\t'

type TestRowDriver struct {
	row []string
}

func NewTestRowDriver(v []string) *TestRowDriver {
	return &TestRowDriver{
		row: v,
	}
}

func (trd *TestRowDriver) GetColumn(idx int) (*RawValue, error) {
	val := trd.row[idx]
	if val == testNullSeq {
		return NewRawValue(nil, true), nil
	}
	return NewRawValue([]byte(val), false), nil
}

func (trd *TestRowDriver) SetColumn(idx int, v *RawValue) error {
	if v.IsNull {
		trd.row[idx] = testNullSeq
	} else {
		trd.row[idx] = string(v.Data)
	}
	return nil
}

func (trd *TestRowDriver) Encode() ([]byte, error) {
	var res []byte
	for idx, v := range trd.row {
		res = append(res, []byte(v)...)
		if idx != len(trd.row)-1 {
			res = append(res, testDelim)
		}
	}
	return res, nil
}

func (trd *TestRowDriver) Decode() ([]*RawValue, error) {
	var res []*RawValue
	for _, v := range trd.row {
		if v == testNullSeq {
			res = append(res, NewRawValue(nil, true))
		} else {
			res = append(res, NewRawValue([]byte(v), false))
		}
	}
	return res, nil
}
