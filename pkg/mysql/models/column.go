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

package models

import (
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

type Column struct {
	Idx               int
	Name              string
	TypeName          string
	DataType          *string
	NumericPrecision  *int
	NumericScale      *int
	DateTimePrecision *int
	NotNull           bool
	TypeID            core.TypeID
	TypeClass         core.TypeClass
}

// toCoreType projects this vendor-shaped introspection column into the canonical
// engine-agnostic core.Type. This is the single place the MySQL flat type
// metadata is turned into a core.Type, so the signedness rule in particular
// lives here and is never re-derived downstream. MySQL records two type strings:
// DATA_TYPE is the canonical base name ("int"), COLUMN_TYPE is the
// fully-declared string with modifiers ("int unsigned"); codec dispatch and
// type-class lookup key on the base name, while the full string carries fidelity
// and is the authoritative source for the sign modifier.
func (c Column) toCoreType() core.Type {
	baseName := c.TypeName
	if c.DataType != nil {
		baseName = *c.DataType
	}
	return core.Type{
		Name:     baseName,
		FullName: c.TypeName,
		ID:       c.TypeID,
		Class:    c.TypeClass,
		// Signedness is a structured fact derived once here from the declared
		// type, never re-parsed from FullName downstream. MySQL also allows
		// DECIMAL/FLOAT/DOUBLE UNSIGNED; the driver simply ignores Unsigned for
		// classes where it does not change the Go type.
		Unsigned:  strings.Contains(strings.ToLower(c.TypeName), "unsigned"),
		Precision: c.NumericPrecision,
		Scale:     c.NumericScale,
	}
}

func NewColumn(
	idx int,
	name, typeName string,
	dataType *string,
	numericPrecision, numericScale, dateTimePrecision *int,
	notNull bool,
	typeOID core.TypeID,
	typeClass core.TypeClass,
) Column {
	return Column{
		Idx:               idx,
		Name:              name,
		TypeName:          typeName,
		DataType:          dataType,
		NumericPrecision:  numericPrecision,
		NumericScale:      numericScale,
		DateTimePrecision: dateTimePrecision,
		NotNull:           notNull,
		TypeID:            typeOID,
		TypeClass:         typeClass,
	}
}
