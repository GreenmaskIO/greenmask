// Copyright 2025 Greenmask
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

package rawrecord

import (
	"bytes"
	"fmt"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
)

type RawRecord struct {
	columnCount  int
	row          [][]byte
	nullValueSeq []byte
}

func NewRawRecord(columnCount int, nullValueSeq []byte) *RawRecord {
	return &RawRecord{
		columnCount:  columnCount,
		row:          make([][]byte, columnCount),
		nullValueSeq: nullValueSeq,
	}
}

func (c *RawRecord) GetColumn(idx int) (*commonmodels.ColumnRawValue, error) {
	if idx < 0 || idx >= c.columnCount {
		return nil, fmt.Errorf("column index %d out of range: %w", idx, commonmodels.ErrUnknownColumnIdx)
	}
	if bytes.Equal(c.nullValueSeq, c.row[idx]) {
		return commonmodels.NewColumnRawValue(nil, true), nil
	}
	return commonmodels.NewColumnRawValue(c.row[idx], false), nil
}

func (c *RawRecord) SetColumn(idx int, v *commonmodels.ColumnRawValue) error {
	if idx < 0 || idx >= c.columnCount {
		return fmt.Errorf("column index %d out of range: %w", idx, commonmodels.ErrUnknownColumnIdx)
	}
	if v.IsNull {
		c.row[idx] = utils.CopyAndExtendIfNeeded(c.row[idx], c.nullValueSeq)
		return nil
	}
	c.row[idx] = utils.CopyAndExtendIfNeeded(c.row[idx], v.Data)
	return nil
}

func (c *RawRecord) SetRow(row [][]byte) error {
	if len(row) != c.columnCount {
		return fmt.Errorf(
			"src length %d is not equal to dst length %d: %w",
			len(row), c.columnCount, commonmodels.ErrProvidedRowLengthIsNotEqualToTheDestination,
		)
	}
	for i := range c.row {
		// Copy from one row to another.
		// If the size of dst if greater than current allocate a new and then copy.
		c.row[i] = utils.CopyAndExtendIfNeeded(c.row[i], row[i])
	}
	return nil
}

func (c *RawRecord) GetRow() [][]byte {
	return c.row
}
