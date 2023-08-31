package utils

import (
	"fmt"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/toclib"
)

type Tuple map[string]any

type Record struct {
	table   *toclib.Table
	rawData []string
	tuple   Tuple
}

func NewRecord(table *toclib.Table, rawData []string) *Record {
	return &Record{
		table:   table,
		tuple:   make(Tuple),
		rawData: rawData,
	}
}

func (r *Record) GetTuple() (Tuple, error) {
	return r.tuple, nil
}

func (r *Record) SetTuple(t Tuple) error {
	r.tuple = t
	return nil
}

func (r *Record) GetAttribute(name string, v any) error {
	// Check attribute exists
	column, ok := r.attrMap[name]
	if !ok {
		return fmt.Errorf("unknown attribute: %s", name)
	}
	// Check attribute already Decoded
	existVal, ok := r.tuple[name]
	if ok {
		// TODO: it will cause error. FInd the way how to assign interface{} to interface{}
		v = &existVal
	}
	idx, ok := r.attrIdxMap[name]
	if !ok {
		panic("wrongly built attrIdxMap")
	}
	// TODO: You should cache pgtype for the attribute of record
	pgtype, ok := r.table.TypeMap.TypeForOID(uint32(column.TypeOid))
	if !ok {
		return fmt.Errorf("cannot retreive pgtype for column %s with type %d", column.Name, column.TypeOid)
	}
	if err := Scan(r.rawData[idx], v, uint32(column.TypeOid), r.table.TypeMap, pgtype); err != nil {
		return fmt.Errorf("scan error: %w", err)
	}
	r.tuple[column.Name] = v
	return nil
}

// SetAttribute - set transformed attribute to the tuple
func (r *Record) SetAttribute(name string, v any) error {
	_, ok := r.attrMap[name]
	if !ok {
		return fmt.Errorf("unknown attribute: %s", name)
	}
	r.tuple[name] = v
	return nil
}

// Encode - filling the record from buf with allocated data
func (r *Record) Encode(buf []byte) ([]byte, error) {
	for name, value := range r.tuple {
		idx := r.attrIdxMap[name]

	}
}

// Decode - decode raw record from CSV to Record
func (r *Record) Decode(rawRecord []string) error {

}
