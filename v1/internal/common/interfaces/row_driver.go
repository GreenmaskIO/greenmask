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

package interfaces

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

// RowDriver - represents methods for interacts with any transferring format
// It might be COPY, CSV, JSON, etc.
// See implementation pgcopy.Row
// RowDriver must keep the current row state
type RowDriver interface {
	// GetColumn - get raw []byte value by column idx
	GetColumn(idx int) (*models.ColumnRawValue, error)
	// SetColumn - set RawValue value by column idx to the current row
	SetColumn(idx int, v *models.ColumnRawValue) error
	// SetRow - sets a row data directly to the RowDriver state.
	// This can be used to override the whole record or
	// to copy a data from driver if it has been provided already split
	// by columns. Can return error if the requested row to replace
	// len if not equal to the current.
	SetRow(row [][]byte) error
	GetRow() [][]byte
}
