package toolkit

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

type Tuple map[string]*Value

type Record struct {
	Driver *Driver
	Row    RowDriver
}

func NewRecord(driver *Driver, row RowDriver) *Record {

	return &Record{
		Driver: driver,
		Row:    row,
	}
}

func (r *Record) GetTuple() (Tuple, error) {
	tuple := make(Tuple, len(r.Driver.Table.Columns))
	for _, c := range r.Driver.Table.Columns {
		v, err := r.GetAttribute(c.Name)
		if err != nil {
			return nil, fmt.Errorf("error getting attribute: %w", err)
		}
		tuple[c.Name] = v
	}
	return tuple, nil
}

//func (r *Record) SetTuple(t Tuple) error {
//	r.mx.Lock()
//	defer r.mx.Unlock()
//	if len(t) != len(r.tuple) {
//		return fmt.Errorf("recieved wrong tuple length")
//	}
//	r.tuple = t
//	return nil
//}

// ScanAttribute - scan data from column with name into v and return isNull property and error
func (r *Record) ScanAttribute(name string, v any) (bool, error) {
	idx, c, ok := r.Driver.GetColumnByName(name)
	if !ok {
		return false, fmt.Errorf(`unknown column name "%s"`, name)
	}
	rawData, err := r.Row.GetColumn(idx)
	if err != nil {
		return false, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.Driver.Table.Schema, r.Driver.Table.Name, c.Name,
			err,
		)
	}

	if rawData.IsNull {
		return true, nil
	} else {
		if err := r.Driver.ScanByTypeOid(uint32(c.TypeOid), rawData.Data, v); err != nil {
			return false, fmt.Errorf("cannot scan: %w", err)
		}
	}
	return false, nil
}

func (r *Record) GetAttribute(name string) (*Value, error) {
	idx, c, ok := r.Driver.GetColumnByName(name)
	if !ok {
		return nil, fmt.Errorf(`unknown column name "%s"`, name)
	}
	rawData, err := r.Row.GetColumn(idx)
	if err != nil {
		return nil, fmt.Errorf(
			"error getting column %s.%s.%s value: %w",
			r.Driver.Table.Schema, r.Driver.Table.Name, c.Name,
			err,
		)
	}
	if rawData.IsNull {
		return NewValue(nil, true), nil
	}
	decodedValue, err := r.Driver.DecodeByTypeOid(uint32(c.TypeOid), rawData.Data)
	if err != nil {
		return nil, fmt.Errorf("error decoding arribute: %w", err)
	}
	return NewValue(decodedValue, false), nil
}

// SetAttribute - set transformed attribute to the tuple
func (r *Record) SetAttribute(name string, v any) error {
	if v == nil {
		return fmt.Errorf("value cannot be nil pointer")
	}
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return fmt.Errorf("unable to find column by name")
	}

	var value *Value
	switch v.(type) {
	case *Value:
	default:
		value = NewValue(v, false)
	}
	if value.IsNull {
		if err := r.Row.SetColumn(idx, NewRawValue(nil, true)); err != nil {
			return fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
	} else {
		encodedValue, err := r.encodeValue(r.Driver.Table.Columns[idx], value.Value)
		if err != nil {
			return fmt.Errorf("unable to encode attr value: %w", err)
		}
		if err = r.Row.SetColumn(idx, NewRawValue(encodedValue, false)); err != nil {
			return fmt.Errorf("error setting column value in RowDriver: %w", err)
		}
	}

	return nil
}

func (r *Record) Encode() (RowDriver, error) {
	return r.Row, nil
}

func (r *Record) encodeValue(c *Column, v any) (res []byte, err error) {

	switch vv := v.(type) {
	case string:
		// We need to encode-decode procedure v that are assigned as string
		// v for non textual attributes
		// TODO: Check condition
		if c.TypeOid != pgtype.VarcharOID && c.TypeOid != pgtype.TextOID && c.TypeOid != pgtype.JSONOID &&
			c.TypeOid != pgtype.JSONBOID {
			decodedVal, err := r.Driver.DecodeAttr(c.Name, []byte(vv))
			if err != nil {
				return nil, fmt.Errorf("unable to force decoding textual v of attribte %s for non textual %s type: %w", c.Name, c.TypeName, err)
			}

			if tn, ok := r.Driver.columnTypeOverrides[c.Name]; ok {
				res, err = r.Driver.EncodeByTypeName(tn, decodedVal, nil)
				if err != nil {
					return nil, fmt.Errorf("encoding error: %w", err)
				}
			} else {
				res, err = r.Driver.EncodeByTypeOid(uint32(c.TypeOid), decodedVal, nil)
				if err != nil {
					return nil, fmt.Errorf("encoding error: %w", err)
				}
			}

		} else {
			res = []byte(vv)
		}

	default:
		res, err = r.Driver.EncodeAttr(c.Name, vv, nil)
		if err != nil {
			return nil, fmt.Errorf("encoding error: %w", err)
		}
	}
	return res, nil
}

func (r *Record) GetRawAttributeValue(name string) (*RawValue, error) {
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("unable to find column by name")
	}
	return r.Row.GetColumn(idx)
}

func (r *Record) SetRawAttributeValueByName(name string, value *RawValue) error {
	idx, ok := r.Driver.AttrIdxMap[name]
	if !ok {
		return fmt.Errorf("unable to find column by name")
	}
	if err := r.Row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}

func (r *Record) SetRawAttributeValueByIdx(idx int, value *RawValue) error {
	if err := r.Row.SetColumn(idx, value); err != nil {
		return fmt.Errorf("error setting raw atribute value: %w", err)
	}
	return nil
}
