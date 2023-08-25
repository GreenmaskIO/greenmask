package transformers

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Driver - allows you to perform decoding operations from []bytes to go types and go types to bytes
// encoding operation
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
}

func NewDriver(typeMap *pgtype.Map, table *Table) (*Driver, error) {
	columnTypes := make(map[string]*pgtype.Type, len(table.Columns))
	columnMap := make(map[string]*Column, len(table.Columns))
	for _, c := range table.Columns {
		columnMap[c.Name] = c
		pgType, ok := typeMap.TypeForOID(uint32(c.TypeOid))
		if !ok {
			return nil, fmt.Errorf("cannot match pgtype for column %s with type %d", c.Name, c.TypeOid)
		}
		columnTypes[c.Name] = pgType
	}

	pc := &Driver{
		TypeMap:     typeMap,
		Table:       table,
		columnTypes: columnTypes,
		ColumnMap:   columnMap,
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

func (d *Driver) DecodeAttr(name string, src []byte, dest any) error {
	var planScan pgtype.ScanPlan
	planScan, ok := d.columnScanPlan[name]
	if !ok {
		pgType, ok := d.columnTypes[name]
		if !ok {
			return fmt.Errorf("unknown column %s", name)
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

func (d *Driver) EncodeByOid(oid uint32, src any, buf []byte) ([]byte, error) {
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

func (d *Driver) EncodeByName(name string, src any, buf []byte) ([]byte, error) {
	pgType, ok := d.TypeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("cannot find type by oid")
	}
	return d.EncodeByOid(pgType.OID, src, buf)
}

func (d *Driver) ScanByOid(oid uint32, src []byte, dest any) error {
	return d.scan(oid, src, dest)
}

func (d *Driver) ScanByName(name string, src []byte, dest any) error {
	pgType, ok := d.TypeMap.TypeForName(name)
	if !ok {
		return fmt.Errorf("cannot find type by oid")
	}
	return d.scan(pgType.OID, src, dest)
}

func (d *Driver) scan(oid uint32, src []byte, dest any) error {
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
