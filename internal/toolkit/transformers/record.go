package transformers

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

// TODO: Need refactoring you should port that implementation to [][]bytes once it COPY parser is implemented

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
	if len(r.tuple) == len(r.driver.Table.Columns) {
		return r.tuple, nil
	} else if len(r.RawData) != len(r.driver.Table.Columns) {
		return nil, fmt.Errorf("wrong rawData length expected %d but got %d", len(r.driver.Table.Columns), len(r.RawData))
	}

	for attName, _ := range r.driver.ColumnMap {
		_, ok := r.tuple[attName]
		if !ok {
			idx, c, ok := r.driver.GetColumnByName(attName)
			if !ok {
				return nil, fmt.Errorf("attribute %s is not found", attName)
			}
			v, err := r.driver.DecodeByTypeOid(uint32(c.TypeOid), []byte(r.RawData[idx]))
			if err != nil {
				return nil, fmt.Errorf("error decoding attribute %s: %w", attName, err)
			}
			r.tuple[attName] = v
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

func (r *Record) ScanAttribute(name string, v any) error {
	val, ok := r.tuple[name]
	if !ok {
		idx, column, ok := r.driver.GetColumnByName(name)
		if !ok {
			return errors.New("unknown column name")
		}
		if err := r.driver.ScanByTypeOid(uint32(column.TypeOid), []byte(r.RawData[idx]), v); err != nil {
			return fmt.Errorf("cannot scan: %w", err)
		}
		r.tuple[name] = v
		return nil
	}
	return scanPointer(val, v)
}

func (r *Record) GetAttribute(name string) (any, error) {
	var err error
	val, ok := r.tuple[name]
	if !ok {
		idx, column, ok := r.driver.GetColumnByName(name)
		if !ok {
			return nil, errors.New("unknown column name")
		}
		val, err = r.driver.DecodeByTypeOid(uint32(column.TypeOid), []byte(r.RawData[idx]))
		if err != nil {
			return nil, fmt.Errorf("decode attr: %w", err)
		}
		r.tuple[name] = val
	}
	return val, nil
}

// SetAttribute - set transformed attribute to the tuple
func (r *Record) SetAttribute(name string, v any) error {
	// TODO: You should check type validity
	r.tuple[name] = v
	return nil
}

// Encode - build CSV record
func (r *Record) Encode() ([]string, error) {
	var err error
	for attrName, value := range r.tuple {
		idx, ok := r.columnIdx[attrName]
		if !ok {
			return nil, fmt.Errorf("unknown column %s", attrName)
		}
		column := r.driver.Table.Columns[idx]
		var res []byte

		switch v := value.(type) {
		case string:
			// We need to encode-decode procedure value that are assigned as string
			// value for non textual attributes
			if v == DefaultNullSeq {
				res = []byte(DefaultNullSeq)
			} else if column.TypeOid != pgtype.VarcharOID && column.TypeOid != pgtype.TextOID {
				decodedVal, err := r.driver.DecodeAttr(attrName, []byte(v))
				if err != nil {
					return nil, fmt.Errorf("unable to force decoding textual value of attribte %s for non textual %s type: %w", attrName, column.TypeName, err)
				}
				res, err = r.driver.EncodeAttr(attrName, decodedVal, nil)
				if err != nil {
					return nil, fmt.Errorf("encoding error: %w", err)
				}
			} else {
				res = []byte(v)
			}

		default:
			res, err = r.driver.EncodeAttr(attrName, value, nil)
			if err != nil {
				return nil, fmt.Errorf("encoding error: %w", err)
			}
		}

		r.RawData[idx] = string(res)
	}
	return r.RawData, nil
}
