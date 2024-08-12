// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

const defaultBufferPoolSize = 128
const defaultDecodedBuf = 1024
const UseDynamicSize = -1

// Row - the row driver that works with vanilla COPY format
type Row struct {
	// raw - the state that received from PG
	raw []byte
	// encoded - bytes that has been encoded using EncodeAttr
	encoded []byte
	// decodeBufferPool
	decodeBufferPool [][]byte
	// encodeBufferPool
	encodeBufferPool [][]byte
	// newValues - raw data that has been assigned in runtime after transformation
	//	those data is after Driver encoding from real type to []byte representation
	newValues []*toolkit.RawValue
	// columnPos - list of the column pos within the raw data
	columnPos []*columnPos
	// columnPos - list of the column pos within the raw data
	tupleSize int
	// isDynamic - flag that indicates that row size will be determined in runtime
	isDynamic bool
}

func NewRow(tupleSize int) *Row {
	var isDynamic bool
	if tupleSize == 0 {
		panic("tuple size should be greater than 0")
	}
	if tupleSize == UseDynamicSize {
		tupleSize = 0
		isDynamic = true
	}
	pos := make([]*columnPos, tupleSize)
	decodeBufferPool := make([][]byte, tupleSize)
	encodeBufferPool := make([][]byte, tupleSize)

	// Building column position slice
	for idx := range pos {
		pos[idx] = &columnPos{}
		decodeBufferPool[idx] = make([]byte, defaultBufferPoolSize)
		encodeBufferPool[idx] = make([]byte, defaultBufferPoolSize)
	}
	return &Row{
		columnPos:        pos,
		newValues:        make([]*toolkit.RawValue, tupleSize),
		decodeBufferPool: decodeBufferPool,
		encodeBufferPool: encodeBufferPool,
		encoded:          make([]byte, 0, defaultDecodedBuf),
		tupleSize:        tupleSize,
		isDynamic:        isDynamic,
	}
}

func (r *Row) Decode(raw []byte) error {
	var colStartPos, colEndPos int

	// Building column position slice
	idx := 0
	for colStartPos <= len(raw) {

		colEndPos = slices.Index(raw[colStartPos:], DefaultCopyDelimiter)
		if colEndPos == -1 {
			colEndPos = len(raw)
		} else {
			colEndPos = colStartPos + colEndPos
		}
		if r.isDynamic && idx >= r.tupleSize {
			r.appendNewEmptyBuffer()
		}

		p := r.columnPos[idx]
		p.start = colStartPos
		p.end = colEndPos

		colStartPos = colEndPos + 1
		idx++
	}
	r.raw = raw
	return nil
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
	res = DecodeAttr(r.raw[pos.start:pos.end], r.decodeBufferPool[idx][:0])
	return res, nil
}

func (r *Row) GetColumnRaw(idx int) ([]byte, error) {
	if len(r.columnPos) <= idx {
		return nil, ErrIndexOutOfRage
	}
	pos := r.columnPos[idx]
	return r.raw[pos.start:pos.end], nil
}

// SetColumn - set column (replace original) value and decode it later
func (r *Row) SetColumn(idx int, v *toolkit.RawValue) error {
	if idx > len(r.columnPos)-1 {
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

	res := r.encoded[:0]
	for idx, pos := range r.columnPos {
		if av := r.newValues[idx]; av != nil {
			// If value was set then encode it and add to result
			v := EncodeAttr(av, r.encodeBufferPool[idx][:0])
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

func (r *Row) appendNewEmptyBuffer() {
	r.columnPos = append(r.columnPos, &columnPos{})
	r.decodeBufferPool = append(r.decodeBufferPool, make([]byte, defaultBufferPoolSize))
	r.encodeBufferPool = append(r.encodeBufferPool, make([]byte, defaultBufferPoolSize))
	r.newValues = append(r.newValues, nil)
	r.tupleSize++
}

func (r *Row) Length() int {
	return len(r.columnPos)
}

func (r *Row) Clean() {
	panic("clean method is not supported")
}
