package custom

import (
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// GetRawRecordDto - create record data transfer object for custom transformer for provided attributes or for the whole
// record if attributes are empty. This is using for transfer original data to CustomCmd transformer
func GetRawRecordDto(r *transformers.Record, attributes map[int]string) (transformers.RawRecordDto, error) {
	res := make(transformers.RawRecordDto, len(attributes))
	if len(attributes) > 0 {
		for idx, name := range attributes {
			v, err := r.GetRawAttributeValue(name)
			if err != nil {
				return nil, fmt.Errorf("error getting raw atribute value: %w", err)
			}
			res[idx] = transformers.NewRawValueDto(string(v.Data), v.IsNull)
		}
	} else {
		for idx, c := range r.Driver.Table.Columns {
			v, err := r.GetRawAttributeValue(c.Name)
			if err != nil {
				return nil, fmt.Errorf("error getting raw atribute value: %w", err)
			}
			res[idx] = transformers.NewRawValueDto(string(v.Data), v.IsNull)
		}
	}
	return res, nil
}

// SetRawRecordDto - set values of attributes in RawRecordDto to provided Record. This is using after receiving
// transformed data from CustomCmd transformer
func SetRawRecordDto(r *transformers.Record, rrd transformers.RawRecordDto) error {
	for idx, v := range rrd {
		if err := r.SetRawAttributeValueByIdx(idx, transformers.NewRawValue([]byte(v.Data), v.IsNull)); err != nil {
			return fmt.Errorf("error setting raw atribute value: %w", err)
		}
	}
	return nil
}
