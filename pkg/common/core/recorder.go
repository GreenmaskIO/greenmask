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

// The Recorder god-interface is decomposed into cohesive leaf interfaces (each
// ≤6 methods) so that consumers can depend on the narrowest slice they actually
// use. Recorder itself is kept as a composite alias of the leaves, so existing
// call sites referencing it by name keep compiling.

// RowCodec is the raw serialization boundary: get/set the whole row as raw
// columns. Stream readers/writers depend on this.
type RowCodec interface {
	SetRow(rawRecord [][]byte) error
	GetRow() [][]byte
}

// ColumnReaderByIdx reads columns addressed by index. Hot-path raw-value
// transformers resolve the column index once in Init and only touch this leaf
// in Transform.
type ColumnReaderByIdx interface {
	IsNullByColumnIdx(columIdx int) (bool, error)
	GetColumnValueByIdx(columnIdx int) (*ColumnValue, error)
	GetRawColumnValueByIdx(columnIdx int) (*ColumnRawValue, error)
	ScanColumnValueByIdx(idx int, v any) (bool, error)
}

// ColumnReaderByName reads columns addressed by name. Used by name-addressed
// transformers, `when` conditions, and templates.
type ColumnReaderByName interface {
	IsNullByColumnName(columName string) (bool, error)
	GetColumnValueByName(columnName string) (*ColumnValue, error)
	GetRawColumnValueByName(columnName string) (*ColumnRawValue, error)
	ScanColumnValueByName(name string, v any) (bool, error)
	GetColumnByName(columnName string) (*Column, error)
}

// ColumnWriterByIdx writes columns addressed by index. Every transformer's
// write-back path depends on this.
type ColumnWriterByIdx interface {
	SetColumnValueByIdx(columnIdx int, v any) error
	SetRawColumnValueByIdx(columnIdx int, value *ColumnRawValue) error
}

// ColumnWriterByName writes columns addressed by name.
type ColumnWriterByName interface {
	SetColumnValueByName(columnName string, v any) error
	SetRawColumnValueByName(columnName string, value *ColumnRawValue) error
}

// Recorder is the full row view handed to a Transformer. It composes the
// segregated leaves; consumers should depend on the narrowest leaf they use.
type Recorder interface {
	RowCodec
	ColumnReaderByIdx
	ColumnReaderByName
	ColumnWriterByIdx
	ColumnWriterByName

	// TableDriver returns the driver for the row's table. Kept inline (not a
	// leaf): no consumer depends on this accessor in isolation — callers chain
	// straight into ColumnCodec/TableSchema on the result. It is an escape
	// hatch, not a segregated capability.
	TableDriver() TableDriver
}
