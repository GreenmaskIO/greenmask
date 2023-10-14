package pgcopy

import (
	"errors"
	"slices"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var ErrIndexOutOfRage = errors.New("wrong column idx: index out of range")

type columnPos struct {
	start int
	end   int
}

// Row - the row driver that works with vanilla COPY format
type Row struct {
	// raw - the state that received from PG
	raw []byte
	// decoded - bytes that has been decoded using DecodeAttr
	decoded []byte
	// newValues - raw data that has been assigned in runtime after transformation
	//	those data is after Driver encoding from real type to []byte representation
	//newValues map[int]*toolkit.RawValue
	newValues []*toolkit.RawValue
	// columnPos - list of the column pos within the raw data
	columnPos []*columnPos
}

func NewRow(tupleSize int) *Row {
	pos := make([]*columnPos, tupleSize)

	// Building column position slice
	for idx, _ := range pos {
		pos[idx] = &columnPos{}
	}
	return &Row{
		columnPos: pos,
		newValues: make([]*toolkit.RawValue, tupleSize),
	}
}

func (r *Row) Parse(raw []byte) {
	var colStartPos, colEndPos int

	// Building column position slice
	idx := 0
	for colStartPos < len(raw) {
		colEndPos = len(raw)

		colEndPos = slices.Index(raw[colStartPos:], DefaultCopyDelimiter)
		if colEndPos == -1 {
			colEndPos = len(raw)
		} else {
			colEndPos = colStartPos + colEndPos
		}

		p := r.columnPos[idx]
		p.start = colStartPos
		p.end = colEndPos

		colStartPos = colEndPos + 1
		idx++
	}
	r.raw = raw
}

// GetColumn - find raw data and encode it using DecodeAttr
func (r *Row) GetColumn(idx int) (*toolkit.RawValue, error) {

	if len(r.columnPos) <= idx {
		return nil, ErrIndexOutOfRage
	}

	res := r.newValues[idx]
	if res != nil {
		return res, nil
	}
	pos := r.columnPos[idx]
	res = DecodeAttr(r.raw[pos.start:pos.end])
	return res, nil
}

// SetColumn - set column (replace original) value and decode it later
func (r *Row) SetColumn(idx int, v *toolkit.RawValue) error {
	if idx > len(r.raw)-1 {
		return ErrIndexOutOfRage
	}
	r.newValues[idx] = v
	return nil
}

// Encode - return encoded bytes from golang representation to COPY format.
// if SetColumn has never been called than original raw data will be returned
func (r *Row) Encode() ([]byte, error) {
	if len(r.newValues) == 0 {
		return r.raw, nil
	}

	res := make([]byte, 0, len(r.raw))
	for idx, pos := range r.columnPos {
		if av := r.newValues[idx]; av != nil {
			// If value was set then encode it and add to result
			v := EncodeAttr(av)
			res = append(res, v...)
			r.newValues[idx] = nil
		} else {
			// Otherwise insert an original value
			res = append(res, r.raw[pos.start:pos.end]...)
		}

		if idx != len(r.columnPos)-1 {
			res = append(res, DefaultCopyDelimiter)
		}
	}
	return res, nil
}

func (r *Row) Decode() (map[int]*toolkit.RawValue, error) {
	res := make(map[int]*toolkit.RawValue, len(r.columnPos))

	for idx, pos := range r.columnPos {
		if av := r.newValues[idx]; av != nil {
			// If value was set then return it
			res[idx] = av
		} else {
			av = DecodeAttr(r.raw[pos.start:pos.end])
			res[idx] = av
		}
	}
	return res, nil
}

func (r *Row) Length() int {
	return len(r.columnPos)
}
