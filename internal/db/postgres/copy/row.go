package copy

import (
	"errors"
	"slices"
)

type columnPos struct {
	start int
	end   int
}

type Row struct {
	// raw - the state that received from PG
	raw []byte
	// decoded - bytes that has been decoded using DecodeAttr
	decoded []byte
	// newValues - raw data that has been assigned in runtime after transformation
	//	those data is after Driver encoding from real type to []byte representation
	newValues map[int]*AttributeValue
	// needParsing - shows that in this run the new value was assigned and
	//	encoding is required for those columns
	needParsing bool
	columnPos   []*columnPos
}

func NewRow(raw []byte) *Row {
	var res []*columnPos

	var colStartPos, colEndPos int

	for colStartPos < len(raw) {

		colEndPos = len(raw)

		colEndPos = slices.Index(raw, defaultCopyDelimiter)
		if colEndPos == -1 {
			colEndPos = len(raw)
		}

		//colVal := DecodeAttr(curPos[colStartPos:colEndPos])
		res = append(res, &columnPos{
			start: colStartPos,
			end:   colStartPos,
		})

		colStartPos = colEndPos + 1
	}
	return &Row{
		raw: raw,
	}
}

func (r *Row) GetColumn(idx int) (*AttributeValue, error) {

	if len(r.columnPos) <= idx {
		return nil, errors.New("wrong column idx: index out of range")
	}

	pos := r.columnPos[idx]
	res := DecodeAttr(r.raw[pos.start:pos.end])
	return res, nil
}

func (r *Row) SetColumn(idx int, v *AttributeValue) {
	r.newValues[idx] = v
	r.needParsing = true
}

func (r *Row) Encode() ([]byte, error) {
	if len(r.newValues) == 0 {
		return r.raw, nil
	}

	res := make([]byte, 0, len(r.raw))
	for idx, pos := range r.columnPos {
		if av, ok := r.newValues[idx]; ok {
			v := EncodeAttr(av)
			res = append(res, v...)
		} else {
			res = append(res, r.raw[pos.start:pos.end]...)
		}

		if idx != len(r.columnPos)-1 {
			res = append(res, defaultCopyDelimiter)
		}
	}
	return res, nil
}

func (r *Row) Decode() ([]*AttributeValue, error) {
	// 1. Split value by the delimiter
	// 2. Decode all those value using DecodeAttr
	var res []*AttributeValue

	for _, pos := range r.columnPos {
		colVal := DecodeAttr(r.raw[pos.start:pos.end])
		res = append(res, colVal)
	}
	return res, nil
}
