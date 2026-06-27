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

// NamedTypeCodec encodes/decodes/scans a value by its canonical type name. It is
// the strict-name codec path for callers that hold a bare type name (e.g.
// template casts like "timestamp"/"int8"); callers holding a full descriptor use
// the TypedCodec path instead. There is deliberately no by-id codec: the type id
// is a field of Type (cross-engine identity, schema diffing) and an internal
// Name-empty fallback, never a public codec dispatch key — dispatching on a bare
// id reintroduces the id-0 footgun the Type-based dispatch avoids.
type NamedTypeCodec interface {
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	ScanValueByTypeName(name string, src []byte, dest any) error
}

// TypedCodec encodes/decodes/scans a value from a full Type descriptor, so
// signedness (and future limits/constraints) drive the codec rather than a bare
// type id or name. It is a distinct leaf from TypeCodec/NamedTypeCodec because
// its key is the self-describing Type, not a scalar id/name; the id/name codecs
// remain for context-less callers and default to the signed integer
// interpretation. It is the canonical codec path the column layer dispatches to,
// so every per-column encode/decode/scan flows through one Type.
type TypedCodec interface {
	EncodeValueByType(t Type, src any, buf []byte) ([]byte, error)
	DecodeValueByType(t Type, src []byte) (any, error)
	ScanValueByType(t Type, src []byte, dest any) error
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
	NamedTypeCodec
	TypedCodec
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
