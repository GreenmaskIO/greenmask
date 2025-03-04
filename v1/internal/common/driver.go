package common

import (
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var (
	ErrorColumnTypeIsNotSupported = errors.New("encode-decode operation is not supported for column")
	ErrorColumnIndexOutOfRange    = errors.New("index out ouf range")
	ErrorUnknownColumnName        = errors.New("unknown column")
)

type DBMSDriver interface {
	EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error)
	EncodeValueByTypeOid(oid uint32, src any, buf []byte) ([]byte, error)
	DecodeValueByTypeName(name string, src []byte) (any, error)
	DecodeValueByTypeOid(oid uint32, src []byte) (any, error)
	ScanValueByTypeName(name string, src []byte, dest any) error
	ScanValueByTypeOid(oid uint32, src []byte, dest any) error
	TypeExistsByName(name string) bool
	TypeExistsByOid(oid uint32) bool
	GetTypeOid(name string) (uint32, error)
}

type TableDriver struct {
	DBMSDriver
	table *Table
	// columnMap - map column name to Column object
	columnMap map[string]*Column
	// columnIdxMap - the number of attribute in tuple
	columnIdxMap map[string]int
	// unsupportedColumnNames - map with unsupported column types that cannot perform encode-decode operations
	unsupportedColumnNames      map[string]string
	unsupportedColumnIdxs       map[int]string
	typeOverride                map[string]string
	columnTypeOidOverrideMap    map[string]uint32
	columnIdxTypeOidOverrideMap map[int]uint32
	maxIdx                      int
}

func NewTableDriver(d DBMSDriver, t *Table, typeOverride map[string]string) (
	*TableDriver, toolkit.ValidationWarnings, error,
) {

	var warnings toolkit.ValidationWarnings
	columnMap := make(map[string]*Column, len(t.Columns))
	attrIdxMap := make(map[string]int, len(t.Columns))
	unsupportedColumnNames := make(map[string]string)
	unsupportedColumnIdxs := make(map[int]string)
	columnTypeOidOverrideMap := make(map[string]uint32)
	columnIdxTypeOidOverrideMap := make(map[int]uint32)

	for idx, c := range t.Columns {
		columnMap[c.Name] = c
		attrIdxMap[c.Name] = idx
		// Check column type is supported by driver
		// TODO: Check it works correctly either
		if !d.TypeExistsByOid(c.TypeOid) {
			log.Debug().
				Str("TableSchema", t.Schema).
				Str("TableName", t.Name).
				Str("ColumnName", c.Name).
				Str("TypeName", c.TypeName).
				Msg("cannot match encoder/decoder for type: encode and decode operations is not supported")
			unsupportedColumnNames[c.Name] = c.TypeName
			unsupportedColumnIdxs[idx] = c.TypeName
		}

		if typeOverride[c.Name] != "" {
			if !d.TypeExistsByName(typeOverride[c.Name]) {
				warnings = append(
					warnings,
					toolkit.NewValidationWarning().
						SetSeverity(toolkit.WarningValidationSeverity).
						SetMsg("unknown or unsupported overridden type name by DBMS driver:"+
							" encode and decode operations are not supported").
						AddMeta("OverriddenColumnName", c.Name).
						AddMeta("OverriddenTypeName", typeOverride[c.Name]),
				)
				unsupportedColumnNames[c.Name] = c.TypeName
				unsupportedColumnIdxs[idx] = c.TypeName
				continue
			}
			oid, err := d.GetTypeOid(typeOverride[c.Name])
			if err != nil {
				return nil, nil, fmt.Errorf("get type oid: %w", err)
			}
			columnTypeOidOverrideMap[c.Name] = oid
			columnIdxTypeOidOverrideMap[idx] = oid
		}
	}

	return &TableDriver{
		DBMSDriver:                  d,
		table:                       t,
		columnMap:                   columnMap,
		columnIdxMap:                attrIdxMap,
		unsupportedColumnNames:      unsupportedColumnNames,
		unsupportedColumnIdxs:       unsupportedColumnIdxs,
		typeOverride:                typeOverride,
		columnTypeOidOverrideMap:    columnTypeOidOverrideMap,
		columnIdxTypeOidOverrideMap: columnIdxTypeOidOverrideMap,
		maxIdx:                      len(t.Columns) - 1,
	}, warnings, nil

}

func (d *TableDriver) EncodeValueByColumnIdx(idx int, src any, buf []byte) ([]byte, error) {
	if err := validateDriverSupportColumnByIdx(d.unsupportedColumnIdxs, idx); err != nil {
		return nil, err
	}
	if err := validateColumnIndexOutOfRange(d.maxIdx, idx); err != nil {
		return nil, err
	}
	var oid uint32
	if overrideOid, ok := d.columnIdxTypeOidOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.DBMSDriver.EncodeValueByTypeOid(oid, src, buf)
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
	var oid uint32
	if overrideOid, ok := d.columnIdxTypeOidOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.DBMSDriver.ScanValueByTypeOid(oid, src, dest)
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
	var oid uint32
	if overrideOid, ok := d.columnIdxTypeOidOverrideMap[idx]; ok {
		oid = overrideOid
	}
	return d.DBMSDriver.DecodeValueByTypeOid(oid, src)
}

func (d *TableDriver) DecodeValueByColumnName(name string, src []byte) (any, error) {
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return nil, fmt.Errorf("name=%s: %w", name, ErrorUnknownColumnName)
	}
	return d.DecodeValueByColumnIdx(idx, src)
}

func (d *TableDriver) GetColumnByName(name string) (int, *Column, bool) {
	v, ok := d.columnMap[name]
	if !ok {
		return 0, nil, false
	}
	idx, ok := d.columnIdxMap[name]
	if !ok {
		return 0, nil, false
	}
	return idx, v, ok
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
