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

package toolkit

// RowDriver - represents methods for interacts with any transferring format
// It might be COPY, CSV, JSON, etc.
// See implementation pgcopy.Row
// RowDriver must keep the current row state
type RowDriver interface {
	// GetColumn - get raw []byte value by column idx
	GetColumn(idx int) (*RawValue, error)
	// SetColumn - set RawValue value by column idx to the current row
	SetColumn(idx int, v *RawValue) error
	// Encode - encode the whole row to the []byte representation of RowDriver. It would be CSV
	// line or JSON object, etc.
	Encode() ([]byte, error)
	// Decode - decode []bytes to RowDriver instance
	Decode([]byte) error
	// Length - count of attributes in the row
	Length() int
	// Clean - clean the state
	Clean()
}
