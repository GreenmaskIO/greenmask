package transformers

import (
	"errors"
	"fmt"
)

type Tuple map[string]any

type Record struct {
	driver    *Driver
	RawData   []string
	tuple     Tuple
	columnIdx map[string]int
}

func NewRecord(driver *Driver, rawData []string) *Record {
	columnIdx := make(map[string]int, len(driver.Table.Columns))
	for idx, c := range driver.Table.Columns {
		columnIdx[c.Name] = idx
	}

	return &Record{
		driver:    driver,
		tuple:     make(Tuple, 24),
		RawData:   rawData,
		columnIdx: columnIdx,
	}
}

func (r *Record) GetTuple() (Tuple, error) {
	return nil, fmt.Errorf("is not implemented")
}

func (r *Record) SetTuple(t Tuple) error {
	if len(t) != len(r.tuple) {
		return fmt.Errorf("recieved wrong tuple length")
	}
	r.tuple = t
	return nil
}

func (r *Record) ScanAttribute(name string, v any) error {
	// Check attribute exists
	val, ok := r.tuple[name]
	if !ok {
		idx, ok := r.columnIdx[name]
		if !ok {
			return errors.New("wrong column name")
		}
		if err := r.driver.ScanByName(name, []byte(r.RawData[idx]), v); err != nil {
			return fmt.Errorf("cannot scan attr by name: %w", err)
		}
		r.tuple[name] = v
		return nil
	}
	return scanPointer(val, v)
}

// SetAttribute - set transformed attribute to the tuple
func (r *Record) SetAttribute(name string, v any) {
	r.tuple[name] = v
}

// Encode - filling the record from buf with allocated data
func (r *Record) Encode(buf []byte) ([]string, error) {
	for name, value := range r.tuple {
		idx, ok := r.columnIdx[name]
		if !ok {
			return nil, fmt.Errorf("unknown column %s", name)
		}
		res, err := r.driver.EncodeAttr(name, value, []byte(r.RawData[idx]))
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
		r.RawData[idx] = string(res)
	}
	return r.RawData, nil
}
