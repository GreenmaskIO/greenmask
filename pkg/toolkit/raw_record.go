package toolkit

import (
	"encoding/json"
	"fmt"
)

// RawRecord - record data transfer object for interaction with custom transformer via PIPE
type RawRecord map[int]*RawValueDto

func (rrd RawRecord) GetColumn(idx int) (*RawValue, error) {
	res, ok := rrd[idx]
	if !ok {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	return NewRawValue([]byte(res.Data), res.IsNull), nil
}

func (rrd RawRecord) SetColumn(idx int, v *RawValue) error {
	_, ok := rrd[idx]
	if !ok {
		return fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	rrd[idx] = NewRawValueDto(string(v.Data), v.IsNull)
	return nil
}

func (rrd RawRecord) Encode() ([]byte, error) {
	res, err := json.Marshal(rrd)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rrd RawRecord) Decode() (map[int]*RawValue, error) {
	res := make(map[int]*RawValue, len(rrd))
	for idx, v := range rrd {
		res[idx] = NewRawValue([]byte(v.Data), v.IsNull)
	}
	return res, nil
}
