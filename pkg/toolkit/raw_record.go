package toolkit

import (
	"fmt"
)

type RawRecord map[int]*RawValue

func NewRawRecord(size int) RawRecord {
	return make(RawRecord, size)
}

func (rr *RawRecord) GetColumn(idx int) (*RawValue, error) {
	res, ok := (*rr)[idx]
	if !ok {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	return res, nil
}

func (rr *RawRecord) SetColumn(idx int, v *RawValue) error {
	(*rr)[idx] = v
	return nil
}

func (rr *RawRecord) Encode() ([]byte, error) {
	res, err := json.Marshal(rr)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rr *RawRecord) Decode(data []byte) error {
	*rr = make(map[int]*RawValue, len(*rr))
	return json.Unmarshal(data, rr)
}

func (rr *RawRecord) Length() int {
	return len(*rr)
}

func (rr *RawRecord) Clean() {
	for key, _ := range *rr {
		delete(*rr, key)
	}
}
