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

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

type Column struct {
	Idx               int
	Name              string
	TypeName          string
	DataType          *string
	NumericPrecision  *int
	NumericScale      *int
	DateTimePrecision *int
	NotNull           bool
	TypeOID           models.VirtualOID
	TypeClass         models.TypeClass
}

func NewColumn(
	idx int,
	name, typeName string,
	dataType *string,
	numericPrecision, numericScale, dateTimePrecision *int,
	notNull bool,
	typeOID models.VirtualOID,
	typeClass models.TypeClass,
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
		TypeOID:           typeOID,
		TypeClass:         typeClass,
	}
}
