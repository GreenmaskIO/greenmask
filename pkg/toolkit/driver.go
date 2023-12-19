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

import (
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

// Driver - allows you to perform decoding operations from []bytes to go types and go types to bytes
// encoding operation
// TODO: Rename it to table Driver
type Driver struct {
	Table         *Table
	TypeMapPool   []*pgtype.Map
	SharedTypeMap *pgtype.Map
	// ColumnMap - map column name to Column object
	ColumnMap map[string]*Column
	// AttrIdxMap - the number of attribute in tuple
	AttrIdxMap map[string]int
	// CustomTypes - list of custom types used in tables
	CustomTypes []*Type
	// columnTypeOverrideOids - map of column type replacements. For instance replace original type
	// INT2 to TEXT for column "data" than in map will be {"data": "text"}
	columnTypeOverrideOids []uint32
	columnTypeOverrides    map[string]string
	// unsupportedColumns - map with unsupported column types that cannot perform encode-decode operations
	unsupportedColumns map[string]string
	mx                 *sync.Mutex
	maxIdx             int
}

func NewDriver(table *Table, customTypes []*Type, columnTypeOverrides map[string]string) (*Driver, error) {
	columnMap := make(map[string]*Column, len(table.Columns))
	attrIdxMap := make(map[string]int, len(table.Columns))
	unsupportedColumns := make(map[string]string)

	typeMapPool := make([]*pgtype.Map, len(table.Columns)+1)
	typeOverrideOids := make([]uint32, len(table.Columns))

	for idx := 0; idx < len(typeMapPool); idx++ {
		tm := pgtype.NewMap()
		if len(customTypes) > 0 {
			TryRegisterCustomTypes(tm, customTypes, false)
		}
		typeMapPool[idx] = tm
	}

	if len(typeMapPool) != len(table.Columns)+1 {
		return nil, fmt.Errorf("type map pool length is not equal to table columns count: expected %d got %d", len(table.Columns)+1, len(typeMapPool))
	}
	for idx, c := range table.Columns {
		columnMap[c.Name] = c
		attrIdxMap[c.Name] = idx
		_, ok := typeMapPool[0].TypeForOID(uint32(c.TypeOid))
		if overriddenType, ok := columnTypeOverrides[c.Name]; ok {
			ot, ok := typeMapPool[0].TypeForName(overriddenType)
			if !ok {
				return nil, fmt.Errorf("overriden type %s does not exist", overriddenType)
			}
			typeOverrideOids[idx] = ot.OID
		}

		if !ok {
			log.Warn().
				Str("TableSchema", table.Schema).
				Str("TableName", table.Name).
				Str("ColumnName", c.Name).
				Str("TypeName", c.TypeName).
				Int("TypeOid", int(c.TypeOid)).
				Msg("cannot match encoder/decoder for type: encode and decode operations is not supported")
			unsupportedColumns[c.Name] = c.TypeName
		}
	}

	if columnTypeOverrides == nil {
		columnTypeOverrides = make(map[string]string)
	}

	pc := &Driver{
		TypeMapPool:            typeMapPool[1:],
		SharedTypeMap:          typeMapPool[0],
		Table:                  table,
		ColumnMap:              columnMap,
		AttrIdxMap:             attrIdxMap,
		columnTypeOverrideOids: typeOverrideOids,
		CustomTypes:            customTypes,
		mx:                     &sync.Mutex{},
		maxIdx:                 len(table.Columns) - 1,
		columnTypeOverrides:    columnTypeOverrides,
	}
	return pc, nil
}

func (d *Driver) EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error) {
	if idx < 0 || idx > d.maxIdx {
		return nil, fmt.Errorf("index out ouf range: must be between 0 and %d received %d", d.maxIdx, idx)
	}
	oid := uint32(d.Table.Columns[idx].TypeOid)
	if overriddenType := d.columnTypeOverrideOids[idx]; overriddenType != 0 {
		oid = overriddenType
	}
	res, err := d.TypeMapPool[idx].Encode(oid, pgx.TextFormatCode, src, buf)
	if err != nil {
		return nil, fmt.Errorf("cannot encode value: %w", err)
	}
	return res, nil
}

func (d *Driver) EncodeValueByColumnName(name string, src any, buf []byte) ([]byte, error) {
	if typeName, ok := d.unsupportedColumns[name]; ok {
		return nil, fmt.Errorf("encode-decode operation is not supported for column %s with type %s", name, typeName)
	}

	idx, ok := d.AttrIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("unoknown column %s", name)
	}
	return d.EncodeValueByColumnIdx(idx, src, buf)
}

func (d *Driver) ScanValueByColumnIdx(idx int, src []byte, dest any) error {
	if idx < 0 || idx > d.maxIdx {
		return fmt.Errorf("index out ouf range: must be between 0 and %d received %d", d.maxIdx, idx)
	}
	oid := uint32(d.Table.Columns[idx].TypeOid)
	if overriddenType := d.columnTypeOverrideOids[idx]; overriddenType != 0 {
		oid = overriddenType
	}
	err := d.TypeMapPool[idx].Scan(oid, pgx.TextFormatCode, src, dest)
	if err != nil {
		return fmt.Errorf("error in scan function: %w", err)
	}
	return nil
}

func (d *Driver) ScanValueByColumnName(name string, src []byte, dest any) error {
	if typeName, ok := d.unsupportedColumns[name]; ok {
		return fmt.Errorf("encode-decode operation is not supported for column %s with type %s", name, typeName)
	}
	idx, ok := d.AttrIdxMap[name]
	if !ok {
		return fmt.Errorf("unoknown column %s", name)
	}
	return d.ScanValueByColumnIdx(idx, src, dest)
}

func (d *Driver) DecodeValueByColumnIdx(idx int, src []byte) (any, error) {
	if idx < 0 || idx > d.maxIdx {
		return nil, fmt.Errorf("index out ouf range: must be between 0 and %d received %d", d.maxIdx, idx)
	}
	oid := uint32(d.Table.Columns[idx].TypeOid)
	if overriddenType := d.columnTypeOverrideOids[idx]; overriddenType != 0 {
		oid = overriddenType
	}
	pgType, ok := d.TypeMapPool[0].TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("unsupported encoding column type %s %d", d.Table.Columns[idx].TypeName, d.Table.Columns[idx].TypeOid)
	}
	v, err := pgType.Codec.DecodeValue(d.TypeMapPool[idx], pgType.OID, pgx.TextFormatCode, src)
	if err != nil {
		return nil, fmt.Errorf("decoding error: %w", err)
	}
	return v, nil
}

func (d *Driver) DecodeValueByColumnName(name string, src []byte) (any, error) {
	idx, ok := d.AttrIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("unoknown column %s", name)
	}
	return d.DecodeValueByColumnIdx(idx, src)
}

func (d *Driver) EncodeValueByTypeOid(oid uint32, src any, buf []byte) ([]byte, error) {
	d.mx.Lock()
	res, err := d.SharedTypeMap.Encode(oid, pgx.TextFormatCode, src, buf)
	d.mx.Unlock()
	if err != nil {
		return nil, fmt.Errorf("cannot encode value: %w", err)
	}
	return res, nil
}

func (d *Driver) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	pgType, ok := d.SharedTypeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	return d.EncodeValueByTypeOid(pgType.OID, src, buf)
}

func (d *Driver) DecodeValueByTypeOid(oid uint32, src []byte) (any, error) {
	pgType, ok := d.SharedTypeMap.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	d.mx.Lock()
	res, err := pgType.Codec.DecodeValue(d.SharedTypeMap, oid, pgx.TextFormatCode, src)
	d.mx.Unlock()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Driver) DecodeValueByTypeName(name string, src []byte) (any, error) {
	pgType, ok := d.SharedTypeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	d.mx.Lock()
	res, err := pgType.Codec.DecodeValue(d.SharedTypeMap, pgType.OID, pgx.TextFormatCode, src)
	d.mx.Unlock()
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Driver) ScanValueByTypeOid(oid uint32, src []byte, dest any) error {
	d.mx.Lock()
	err := d.SharedTypeMap.Scan(oid, pgx.TextFormatCode, src, dest)
	d.mx.Unlock()
	if err != nil {
		return fmt.Errorf("unnable to scan: %w", err)
	}
	return nil
}

func (d *Driver) ScanValueByTypeName(name string, src []byte, dest any) error {
	pgType, ok := d.SharedTypeMap.TypeForName(name)
	if !ok {
		return fmt.Errorf("cannot find type by oid")
	}
	d.mx.Lock()
	planScan := pgType.Codec.PlanScan(d.SharedTypeMap, pgType.OID, pgx.TextFormatCode, dest)
	d.mx.Unlock()
	if planScan == nil {
		return fmt.Errorf("cannot find scanner for the type %d", pgType.OID)
	}
	if err := planScan.Scan(src, dest); err != nil {
		return fmt.Errorf("unnable to scan: %w", err)
	}
	return nil
}

func (d *Driver) GetColumnByName(name string) (int, *Column, bool) {
	v, ok := d.ColumnMap[name]
	if !ok {
		return 0, nil, false
	}
	idx, ok := d.AttrIdxMap[name]
	if !ok {
		return 0, nil, false
	}
	return idx, v, ok
}
