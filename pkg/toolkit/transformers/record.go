package transformers

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
)

type Tuple map[string]*Value

type Record struct {
	driver *Driver
	Row    RowDriver
	tuple  Tuple
}

func NewRecord(driver *Driver, row RowDriver) *Record {

	return &Record{
		driver: driver,
		tuple:  make(Tuple, 24),
		Row:    row,
	}
}

func (r *Record) GetTuple() (Tuple, error) {
	if len(r.tuple) == len(r.driver.Table.Columns) {
		return r.tuple, nil
	}

	for _, c := range r.driver.Table.Columns {
		if _, ok := r.tuple[c.Name]; !ok {
			if _, err := r.GetAttribute(c.Name); err != nil {
				return nil, fmt.Errorf("error getting attribute: %w", err)
			}
		}
	}
	return r.tuple, nil
}

func (r *Record) SetTuple(t Tuple) error {
	if len(t) != len(r.tuple) {
		return fmt.Errorf("recieved wrong tuple length")
	}
	r.tuple = t
	return nil
}

// ScanAttribute - scan data from column with name into v and return isNull property and error
func (r *Record) ScanAttribute(name string, v any) (bool, error) {
	val, ok := r.tuple[name]
	if ok {
		if val.IsNull {
			return true, nil
		}
		return false, scanPointer(val, val.Value)
	}

	idx, c, ok := r.driver.GetColumnByName(name)
	if !ok {
		return false, fmt.Errorf(`unknown column name "%s"`, name)
	}
	rawData, err := r.Row.GetColumn(idx)
	if err != nil {
		return false, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.driver.Table.Schema, r.driver.Table.Name, c.Name,
			err,
		)
	}
	if rawData.IsNull {
		r.tuple[name] = NewValue(v, true)
	} else {
		if err := r.driver.ScanByTypeOid(uint32(c.TypeOid), rawData.Data, v); err != nil {
			return false, fmt.Errorf("cannot scan: %w", err)
		}
		r.tuple[name] = NewValue(v, false)
	}
	return false, nil
}

func (r *Record) GetAttribute(name string) (*Value, error) {
	val, ok := r.tuple[name]
	if !ok {
		idx, c, ok := r.driver.GetColumnByName(name)
		if !ok {
			return nil, fmt.Errorf(`unknown column name "%s"`, name)
		}
		rawData, err := r.Row.GetColumn(idx)
		if err != nil {
			return nil, fmt.Errorf(
				"error getting column %s.%s.%s value: %w",
				r.driver.Table.Schema, r.driver.Table.Name, c.Name,
				err,
			)
		}
		if rawData.IsNull {
			val = NewValue(nil, true)
		} else {
			decodedValue, err := r.driver.DecodeByTypeOid(uint32(c.TypeOid), rawData.Data)
			if err != nil {
				return nil, fmt.Errorf("error decoding arribute: %w", err)
			}
			val = NewValue(decodedValue, false)
		}
		r.tuple[name] = val
	}
	return val, nil
}

// SetAttribute - set transformed attribute to the tuple
func (r *Record) SetAttribute(name string, v any) error {
	if v == nil {
		return fmt.Errorf("value cannot be nil pointer")
	}
	switch vv := v.(type) {
	case *Value:
		r.tuple[name] = vv
	default:
		r.tuple[name] = NewValue(v, false)
	}
	return nil
}

func (r *Record) Encode() (RowDriver, error) {
	for name, v := range r.tuple {
		idx, ok := r.driver.AttrIdxMap[name]
		if !ok {
			return nil, fmt.Errorf("unable to find column by name")
		}
		if v.IsNull {
			if err := r.Row.SetColumn(idx, NewRawValue(nil, true)); err != nil {
				return nil, fmt.Errorf("error setting column value in RowDriver: %w", err)
			}
		} else {
			encodedValue, err := r.encodeValue(r.driver.Table.Columns[idx], v.Value)
			if err != nil {
				return nil, fmt.Errorf("unable to encode attr value: %w", err)
			}
			if err = r.Row.SetColumn(idx, NewRawValue(encodedValue, false)); err != nil {
				return nil, fmt.Errorf("error setting column value in RowDriver: %w", err)
			}
		}
	}
	return r.Row, nil
}

func (r *Record) encodeValue(c *Column, v any) (res []byte, err error) {

	switch vv := v.(type) {
	case string:
		// We need to encode-decode procedure v that are assigned as string
		// v for non textual attributes
		if c.TypeOid != pgtype.VarcharOID && c.TypeOid != pgtype.TextOID {
			decodedVal, err := r.driver.DecodeAttr(c.Name, []byte(vv))
			if err != nil {
				return nil, fmt.Errorf("unable to force decoding textual v of attribte %s for non textual %s type: %w", c.Name, c.TypeName, err)
			}
			res, err = r.driver.EncodeAttr(c.Name, decodedVal, nil)
			if err != nil {
				return nil, fmt.Errorf("encoding error: %w", err)
			}
		} else {
			res = []byte(vv)
		}

	default:
		res, err = r.driver.EncodeAttr(c.Name, vv, nil)
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
	}
	return res, nil
}

func (r *Record) GetRawAttributeValue(name string) (*RawValue, error) {
	var res *RawValue
	v, ok := r.tuple[name]
	if ok {
		idx, ok := r.driver.AttrIdxMap[name]
		if !ok {
			return nil, fmt.Errorf("unable to find column by name")
		}
		if v.IsNull {
			res = NewRawValue(nil, true)
		} else {
			encodedValue, err := r.encodeValue(r.driver.Table.Columns[idx], v.Value)
			if err != nil {
				return nil, fmt.Errorf("unable to encode attr value: %w", err)
			}
			res = NewRawValue(encodedValue, false)
		}

		err := r.Row.SetColumn(idx, res)
		if err != nil {
			return nil, fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
		delete(r.tuple, name)
	}
	return res, nil
}

func (r *Record) GetRawRecordDto(attributes ...string) (RawRecordDto, error) {
	res := make(RawRecordDto, len(attributes))
	if len(attributes) > 0 {
		for _, name := range attributes {
			v, err := r.GetRawAttributeValue(name)
			if err != nil {
				return nil, fmt.Errorf("error getting raw atribute value: %w", err)
			}
			res[name] = NewRawValueDto(string(v.Data), v.IsNull)
		}
	} else {
		for _, c := range r.driver.Table.Columns {
			v, err := r.GetRawAttributeValue(c.Name)
			if err != nil {
				return nil, fmt.Errorf("error getting raw atribute value: %w", err)
			}
			res[c.Name] = NewRawValueDto(string(v.Data), v.IsNull)
		}
	}
	return res, nil
}

func (r *Record) SetRawRecordDto(rrd RawRecordDto) error {
	for name, value := range rrd {
		idx, ok := r.driver.AttrIdxMap[name]
		if !ok {
			return fmt.Errorf("unable to find column by name")
		}
		if err := r.Row.SetColumn(idx, NewRawValue([]byte(value.Data), value.IsNull)); err != nil {
			return fmt.Errorf("error setting raw atribute value: %w", err)
		}
	}
	return nil
}
