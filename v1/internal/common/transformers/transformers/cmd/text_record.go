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

package cmd

import (
	"errors"
	"fmt"
	"slices"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var DefaultNullSeq = []byte("\\N")

func isNullSeq(v []byte) bool {
	if len(v) != 2 {
		return false
	}
	return v[0] == DefaultNullSeq[0] && v[1] == DefaultNullSeq[2]
}

var (
	errMoreThanOneColumn = errors.New("more than one column is not allowed")
)

type TextRecord struct {
	transferColumnIdx int
	receiverColumnIdx int
	record            []byte
}

func NewTextRecord(
	transfer []*ColumnMapping,
	receive []*ColumnMapping,
) (*TextRecord, error) {
	if len(transfer) > 1 || len(receive) > 1 {
		return nil,
			fmt.Errorf(
				"use another interaction proto (json or csv): text intearaction proto supports only 1 "+
					"attribute in the payload: got transferring %d affected %d: %w",
				len(transfer), len(receive), errMoreThanOneColumn,
			)
	}
	return &TextRecord{
		transferColumnIdx: transfer[0].Column.Idx,
		receiverColumnIdx: receive[0].Column.Idx,
	}, nil
}

func (r *TextRecord) Encode() ([]byte, error) {
	return r.record, nil
}

func (r *TextRecord) Decode(data []byte) error {
	r.record = data
	return nil
}

func (r *TextRecord) Clean() {
	r.record = r.record[:0]
}

func (r *TextRecord) GetColumn(c *commonmodels.Column) (*commonmodels.ColumnRawValue, error) {
	if c.Idx != r.receiverColumnIdx {
		return nil, fmt.Errorf("column idx=%d name=%s: %w", c.Idx, c.Name, errUnexpectedColumn)
	}
	if isNullSeq(r.record) {
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	return commonmodels.NewColumnRawValue(r.record, false), nil
}

func (r *TextRecord) SetColumn(c *commonmodels.Column, v *commonmodels.ColumnRawValue) error {
	if c.Idx != r.transferColumnIdx {
		return fmt.Errorf("column idx=%d name=%s: %w", c.Idx, c.Name, errUnexpectedColumn)
	}
	if v.IsNull {
		r.record = DefaultNullSeq
		return nil
	}
	r.record = slices.Clone(v.Data)
	return nil
}
