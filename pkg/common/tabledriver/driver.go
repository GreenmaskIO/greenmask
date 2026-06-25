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

package tabledriver

import (
	"context"
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

var (
	_ core.TableDriver = (*TableDriver)(nil)
)

var (
	ErrorColumnTypeIsNotSupported     = errors.New("encode-decode operation is not supported for column")
	ErrorColumnIndexOutOfRange        = errors.New("index out ouf range")
	ErrorUnknownColumnName            = errors.New("unknown column")
	ErrorCannotMatchColumnIdxToTypeID = errors.New("cannot match column index to type OID")
)

type TableDriver struct {
	core.DBMSDriver
	table *core.Table
	// columnMap - map column name to Column object
	columnMap map[string]*core.Column
	// columnIdxToTypeID - map with column index to its type OID
	columnIdxToTypeID map[int]core.TypeID
	// columnIdxMap - the number of attributes in tuple
	columnIdxMap map[string]int
	// unsupportedColumnNames - map with unsupported column types that cannot perform encode-decode operations
	unsupportedColumnNames map[string]string
	// unsupportedColumnIdxs - map with unsupported column types by index that cannot perform encode-decode operations
	unsupportedColumnIdxs map[int]string
	// typeOverride - map with column names and their overridden types.
	typeOverride map[string]string
	// columnTypeIDOverrideMap - map with column names and their overridden types by OID.
	columnTypeIDOverrideMap map[string]core.TypeID
	// columnIdxTypeIDOverrideMap - map with column indexes and their overridden types by OID.
	columnIdxTypeIDOverrideMap map[int]core.TypeID
	// maxIdx - the maximum index of the column in the table.
	maxIdx int
}

func New(
	ctx context.Context,
	d core.DBMSDriver,
	t *core.Table,
	typeOverride map[string]string,
) (*TableDriver, error) {

	columnMap := make(map[string]*core.Column, len(t.Columns))
	columnIdxToTypeID := make(map[int]core.TypeID, len(t.Columns))
	columnIdxMap := make(map[string]int, len(t.Columns))
	unsupportedColumnNames := make(map[string]string)
	unsupportedColumnIdxs := make(map[int]string)
	columnTypeIDOverrideMap := make(map[string]core.TypeID)
	columnIdxTypeIDOverrideMap := make(map[int]core.TypeID)

	for idx, c := range t.Columns {
		columnMap[c.Name] = &c
		columnIdxMap[c.Name] = idx
		columnIdxToTypeID[idx] = c.TypeID
		// Check column type is supported by driver
		if !d.TypeExistsByID(c.TypeID) && typeOverride[c.Name] == "" {
			validationcollector.FromContext(ctx).Add(
				core.NewValidationWarning().
					AddMeta("TableSchema", t.Schema).
					AddMeta("TableName", t.Name).
					AddMeta("ColumnName", c.Name).
					AddMeta("ColumnType", c.TypeName).
					SetSeverity(core.ValidationSeverityWarning).
					SetMsg("cannot match encoder/decoder for type: encode and decode operations is not supported"),
			)
			unsupportedColumnNames[c.Name] = c.TypeName
			unsupportedColumnIdxs[idx] = c.TypeName
		}

		if typeOverride[c.Name] != "" {
			if !d.TypeExistsByName(typeOverride[c.Name]) {
				// In case type is overridden but does not exist in DBMS driver
				// we consider it as a fatal error.
				validationcollector.FromContext(ctx).Add(
					core.NewValidationWarning().
						SetSeverity(core.ValidationSeverityError).
						SetMsg("unknown or unsupported overridden type name by DBMS driver:"+
							" encode and decode operations are not supported").
						AddMeta("OverriddenColumnName", c.Name).
						AddMeta("OverriddenTypeName", typeOverride[c.Name]),
				)
				unsupportedColumnNames[c.Name] = c.TypeName
				unsupportedColumnIdxs[idx] = c.TypeName
				continue
			}
			oid, err := d.GetTypeID(typeOverride[c.Name])
			if err != nil {
				return nil, fmt.Errorf("get type oid: %w", err)
			}
			columnTypeIDOverrideMap[c.Name] = oid
			columnIdxTypeIDOverrideMap[idx] = oid
			columnIdxToTypeID[idx] = oid
		}
	}

	return &TableDriver{
		DBMSDriver:                 d,
		table:                      t,
		columnMap:                  columnMap,
		columnIdxMap:               columnIdxMap,
		unsupportedColumnNames:     unsupportedColumnNames,
		unsupportedColumnIdxs:      unsupportedColumnIdxs,
		typeOverride:               typeOverride,
		columnTypeIDOverrideMap:    columnTypeIDOverrideMap,
		columnIdxTypeIDOverrideMap: columnIdxTypeIDOverrideMap,
		maxIdx:                     len(t.Columns) - 1,
		columnIdxToTypeID:          columnIdxToTypeID,
	}, nil

}

func (d *TableDriver) EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error) {
	if err := validateDriverSupportColumnByIdx(d.unsupportedColumnIdxs, idx); err != nil {
		return nil, err
	}
	if err := validateColumnIndexOutOfRange(d.maxIdx, idx); err != nil {
		return nil, err
	}
	oid, ok := d.columnIdxToTypeID[idx]
	if !ok {
		return nil, ErrorCannotMatchColumnIdxToTypeID
	}
	if overrideOid, ok := d.columnIdxTypeIDOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.EncodeValueByTypeID(oid, src, buf)
}

func (d *TableDriver) EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error) {
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("name=%s: %w", name, ErrorUnknownColumnName)
	}
	return d.EncodeValueByColumnIdx(idx, src, buf)
}

func (d *TableDriver) ScanValueByColumnIdx(idx int, src []byte, dest any) error {
	if err := validateDriverSupportColumnByIdx(d.unsupportedColumnIdxs, idx); err != nil {
		return err
	}
	if err := validateColumnIndexOutOfRange(d.maxIdx, idx); err != nil {
		return err
	}
	oid, ok := d.columnIdxToTypeID[idx]
	if !ok {
		return ErrorCannotMatchColumnIdxToTypeID
	}
	if overrideOid, ok := d.columnIdxTypeIDOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.ScanValueByTypeID(oid, src, dest)
}

func (d *TableDriver) ScanValueByColumnName(name string, src []byte, dest any) error {
	if err := validateDriverSupportColumnByName(d.unsupportedColumnNames, name); err != nil {
		return err
	}
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return fmt.Errorf("name=%s: %w", name, ErrorUnknownColumnName)
	}
	return d.ScanValueByColumnIdx(idx, src, dest)
}

func (d *TableDriver) DecodeValueByColumnIdx(idx int, src []byte) (any, error) {
	if err := validateDriverSupportColumnByIdx(d.unsupportedColumnIdxs, idx); err != nil {
		return nil, err
	}
	if err := validateColumnIndexOutOfRange(d.maxIdx, idx); err != nil {
		return nil, err
	}
	oid, ok := d.columnIdxToTypeID[idx]
	if !ok {
		return nil, ErrorCannotMatchColumnIdxToTypeID
	}
	if overrideOid, ok := d.columnIdxTypeIDOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.DecodeValueByTypeID(oid, src)
}

func (d *TableDriver) DecodeValueByColumnName(name string, src []byte) (any, error) {
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("name=%s: %w", name, ErrorUnknownColumnName)
	}
	return d.DecodeValueByColumnIdx(idx, src)
}

func (d *TableDriver) GetColumnByName(name string) (*core.Column, error) {
	v, ok := d.columnMap[name]
	if !ok {
		return nil, core.ErrUnknownColumnName
	}
	return v, nil
}

func (d *TableDriver) GetColumnIdxByName(name string) (int, error) {
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return 0, core.ErrUnknownColumnName
	}
	return idx, nil
}

func (d *TableDriver) Table() *core.Table {
	return d.table
}

func validateDriverSupportColumnByIdx(unsupportedColumnIdxs map[int]string, idx int) error {
	if typeName, ok := unsupportedColumnIdxs[idx]; ok {
		return fmt.Errorf(
			"column idx=%d with type %s is not supported by driver: %w",
			idx, typeName, ErrorColumnTypeIsNotSupported,
		)
	}
	return nil
}

func validateDriverSupportColumnByName(unsupportedColumnNames map[string]string, name string) error {
	if typeName, ok := unsupportedColumnNames[name]; ok {
		return fmt.Errorf(
			"column %s with type %s is not supported by driver: %w",
			name, typeName, ErrorColumnTypeIsNotSupported,
		)
	}
	return nil
}

func validateColumnIndexOutOfRange(maxIdx int, idx int) error {
	if idx < 0 || idx > maxIdx {
		return fmt.Errorf("requested idx=%d maxIdx=%d: %w", idx, maxIdx, ErrorColumnIndexOutOfRange)
	}
	return nil
}
