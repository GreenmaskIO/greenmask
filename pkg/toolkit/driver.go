package toolkit

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Driver - allows you to perform decoding operations from []bytes to go types and go types to bytes
// encoding operation
// TODO: Rename it to table Driver
type Driver struct {
	Table   *Table
	TypeMap *pgtype.Map
	// ColumnMap - map column name to Column object
	ColumnMap map[string]*Column
	// AttrIdxMap - the number of attribute in tuple
	AttrIdxMap map[string]int
	// columnTypes - column name to the pgx type
	columnTypes map[string]*pgtype.Type
	// columnEncodePlans - cached encode plans for table
	columnEncodePlans map[string]pgtype.EncodePlan
	// columnScanPlan - cached scan plans for table
	columnScanPlan map[string]pgtype.ScanPlan
	// columnTypeOverrides - map of column type replacements. For instance replace original type
	// INT2 to TEXT for column "data" than in map will be {"data": "text"}
	columnTypeOverrides map[string]string
}

func NewDriver(typeMap *pgtype.Map, table *Table, columnTypeOverrides map[string]string) (*Driver, error) {
	columnTypes := make(map[string]*pgtype.Type, len(table.Columns))
	columnMap := make(map[string]*Column, len(table.Columns))
	attrIdxMap := make(map[string]int, len(table.Columns))
	for idx, c := range table.Columns {
		columnMap[c.Name] = c
		attrIdxMap[c.Name] = idx
		pgType, ok := typeMap.TypeForOID(uint32(c.TypeOid))
		if overriddenType, ok := columnTypeOverrides[c.Name]; ok {
			pgType, ok = typeMap.TypeForName(overriddenType)
			if !ok {
				return nil, fmt.Errorf("overriden type %s does not exist", overriddenType)
			}
		}

		if !ok {
			return nil, fmt.Errorf("cannot match pgtype for column %s with type %d", c.Name, c.TypeOid)
		}
		columnTypes[c.Name] = pgType
	}

	if columnTypeOverrides == nil {
		columnTypeOverrides = make(map[string]string)
	}

	pc := &Driver{
		TypeMap:             typeMap,
		Table:               table,
		columnTypes:         columnTypes,
		ColumnMap:           columnMap,
		AttrIdxMap:          attrIdxMap,
		columnEncodePlans:   make(map[string]pgtype.EncodePlan, len(table.Columns)),
		columnTypeOverrides: columnTypeOverrides,
	}
	return pc, nil
}

func (d *Driver) EncodeAttr(name string, src any, buf []byte) ([]byte, error) {
	encodePlan, ok := d.columnEncodePlans[name]
	if !ok {
		pgType, ok := d.columnTypes[name]
		if !ok {
			return nil, fmt.Errorf("unoknown column %s", name)
		}

		encodePlan = d.TypeMap.PlanEncode(pgType.OID, pgx.TextFormatCode, src)
		if encodePlan == nil {
			return nil, errors.New("cannot find encode plan")
		}
		d.columnEncodePlans[name] = encodePlan
	}

	res, err := encodePlan.Encode(src, buf)
	if err != nil {
		return nil, fmt.Errorf("cannot encode value: %w", err)
	}
	return res, nil
}

func (d *Driver) ScanAttr(name string, src []byte, dest any) error {
	var planScan pgtype.ScanPlan
	planScan, ok := d.columnScanPlan[name]
	if !ok {
		var pgType *pgtype.Type
		if overriddenType, ok := d.columnTypeOverrides[name]; ok {
			pgType, ok = d.columnTypes[overriddenType]
			if !ok {
				return fmt.Errorf("overriden type %s does not exist", overriddenType)
			}
		} else {
			pgType, ok = d.columnTypes[name]
			if !ok {
				return fmt.Errorf("unoknown column %s", name)
			}
		}

		planScan = pgType.Codec.PlanScan(d.TypeMap, pgType.OID, pgx.TextFormatCode, dest)
		if planScan == nil {
			return fmt.Errorf("cannot find scanner for the type")
		}
		d.columnScanPlan[name] = planScan
	}
	if err := planScan.Scan(src, dest); err != nil {
		return fmt.Errorf("error in scan function: %w", err)
	}
	return nil
}

func (d *Driver) DecodeAttr(name string, src []byte) (any, error) {
	pgType, ok := d.columnTypes[name]
	if !ok {
		return nil, fmt.Errorf("unknown column %s", name)
	}
	v, err := pgType.Codec.DecodeValue(d.TypeMap, pgType.OID, pgx.TextFormatCode, src)
	if err != nil {
		return nil, fmt.Errorf("decoding error: %w", err)
	}
	return v, nil
}

func (d *Driver) EncodeByTypeOid(oid uint32, src any, buf []byte) ([]byte, error) {
	plan := d.TypeMap.PlanEncode(oid, pgx.TextFormatCode, src)
	if plan == nil {
		return nil, fmt.Errorf("cannot find encoding plan")
	}
	res, err := plan.Encode(src, buf)
	if err != nil {
		return nil, fmt.Errorf("cannot encode value: %w", err)
	}
	return res, nil
}

func (d *Driver) EncodeByTypeName(name string, src any, buf []byte) ([]byte, error) {
	pgType, ok := d.TypeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	return d.EncodeByTypeOid(pgType.OID, src, buf)
}

func (d *Driver) DecodeByTypeOid(oid uint32, src []byte) (any, error) {
	pgType, ok := d.TypeMap.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	res, err := pgType.Codec.DecodeValue(d.TypeMap, oid, pgx.TextFormatCode, src)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Driver) DecodeByTypeName(name string, src []byte) (any, error) {
	pgType, ok := d.TypeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	res, err := pgType.Codec.DecodeValue(d.TypeMap, pgType.OID, pgx.TextFormatCode, src)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (d *Driver) ScanByTypeOid(oid uint32, src []byte, dest any) error {
	pgType, ok := d.TypeMap.TypeForOID(oid)
	if !ok {
		return fmt.Errorf("cannot find type by oid")
	}
	planScan := pgType.Codec.PlanScan(d.TypeMap, oid, pgx.TextFormatCode, dest)
	if planScan == nil {
		return fmt.Errorf("cannot find scanner for the type")
	}
	if err := planScan.Scan(src, dest); err != nil {
		return fmt.Errorf("unnable to scan: %w", err)
	}
	return nil
}

func (d *Driver) ScanByTypeName(name string, src []byte, dest any) error {
	pgType, ok := d.TypeMap.TypeForName(name)
	if !ok {
		return fmt.Errorf("cannot find type by oid")
	}
	planScan := pgType.Codec.PlanScan(d.TypeMap, pgType.OID, pgx.TextFormatCode, dest)
	if planScan == nil {
		return fmt.Errorf("cannot find scanner for the type")
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
