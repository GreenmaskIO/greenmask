package toolkit

import "fmt"

// RawRecordDto - record data transfer object for interaction with custom transformer via PIPE
type RawRecordDto map[int]*RawValueDto

func (rrd RawRecordDto) GetColumn(idx int) (*RawValue, error) {
	res, ok := rrd[idx]
	if !ok {
		return nil, fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	var data []byte
	if res.Data != nil {
		data = []byte(*res.Data)
	}
	return NewRawValue(data, res.IsNull), nil
}

func (rrd RawRecordDto) SetColumn(idx int, v *RawValue) error {
	_, ok := rrd[idx]
	if !ok {
		return fmt.Errorf("attribute with idx=%d is not found", idx)
	}
	rrd[idx] = NewRawValueDto(v.Data, v.IsNull)
	return nil
}

func (rrd RawRecordDto) Encode() ([]byte, error) {
	res, err := json.Marshal(rrd)
	if err != nil {
		return nil, fmt.Errorf("error encoding: %w", err)
	}
	return res, nil
}

func (rrd RawRecordDto) Decode(data []byte) error {
	rrd = make(map[int]*RawValueDto, len(rrd))
	for idx, v := range rrd {
		var data []byte
		if v.Data != nil {
			data = []byte(*v.Data)
		}
		rrd[idx] = NewRawValueDto(data, v.IsNull)
	}
	return nil
}

func (rrd RawRecordDto) Length() int {
	return len(rrd)
}
