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

// The DBMSDriver/TableDriver god-interfaces are decomposed into cohesive leaf
// interfaces (each ≤6 methods) so a new engine implements only the type-level
// codecs (its real work — wire format + type catalog) and obtains the column
// layer for free from the shared pkg/common/tabledriver impl. DBMSDriver and
// TableDriver are kept as composite aliases so existing references compile.

// TypeCodec encodes/decodes/scans a value by its type id.
type TypeCodec interface {
	EncodeValueByTypeID(id TypeID, src any, buf []byte) ([]byte, error)
	DecodeValueByTypeID(id TypeID, src []byte) (any, error)
	ScanValueByTypeID(id TypeID, src []byte, dest any) error
}

// NamedTypeCodec encodes/decodes/scans a value by its type name.
type NamedTypeCodec interface {
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	ScanValueByTypeName(name string, src []byte, dest any) error
}

// TypeIntrospection answers questions about the engine's type catalog.
type TypeIntrospection interface {
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

// DBMSDriver is the type-level driver an engine must implement. It composes the
// segregated type-level leaves; consumers should depend on the narrowest leaf.
type DBMSDriver interface {
	TypeCodec
	NamedTypeCodec
	TypeIntrospection
}

// ColumnCodec encodes/decodes/scans a value by its column index.
type ColumnCodec interface {
	EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error)
	DecodeValueByColumnIdx(idx int, src []byte) (any, error)
	ScanValueByColumnIdx(idx int, src []byte, dest any) error
}

// NamedColumnCodec encodes/decodes/scans a value by its column name.
type NamedColumnCodec interface {
	EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error)
	DecodeValueByColumnName(name string, src []byte) (any, error)
	ScanValueByColumnName(name string, src []byte, dest any) error
}

// TableSchema exposes the table's column layout.
type TableSchema interface {
	Table() *Table
	GetColumnByName(name string) (*Column, error)
	GetColumnIdxByName(name string) (int, error)
}

// TableDriver is the full table-bound driver. It composes the type-level
// DBMSDriver with the column-level leaves; consumers should depend on the
// narrowest leaf they use.
type TableDriver interface {
	DBMSDriver
	ColumnCodec
	NamedColumnCodec
	TableSchema
}
