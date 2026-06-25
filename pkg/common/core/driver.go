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

package core

type DBMSDriver interface {
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
	EncodeValueByTypeID(id TypeID, src any, buf []byte) ([]byte, error)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	DecodeValueByTypeID(id TypeID, src []byte) (any, error)
	ScanValueByTypeName(name string, src []byte, dest any) error
	ScanValueByTypeID(id TypeID, src []byte, dest any) error
	TypeExistsByName(name string) bool
	TypeExistsByID(id TypeID) bool
	GetTypeID(name string) (TypeID, error)
	// GetCanonicalTypeName - returns a canonical type name of a type provided.
	// For example if provided timestamp without timezone the value timestamp must be received.
	// Each DBMS has their own type aliases so this method must return a canonical type for any existing alias
	// or an error if not found. If not found must return ErrCanonicalTypeMismatch.
	GetCanonicalTypeName(typeName string, typeID TypeID) (string, error)
	GetCanonicalTypeClassName(typeName string, typeID TypeID) (TypeClass, error)
}

type TableDriver interface {
	DBMSDriver

	EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error)
	EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error)
	ScanValueByColumnIdx(idx int, src []byte, dest any) error
	ScanValueByColumnName(name string, src []byte, dest any) error
	DecodeValueByColumnIdx(idx int, src []byte) (any, error)
	DecodeValueByColumnName(name string, src []byte) (any, error)
	GetColumnByName(name string) (*Column, error)
	Table() *Table
	GetColumnIdxByName(name string) (int, error)
}
