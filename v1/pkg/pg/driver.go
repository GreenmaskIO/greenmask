package pg

import (
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrorTypeNotFound = fmt.Errorf("type not found")
)

type Driver struct {
	typeMap *pgtype.Map
}

func NewDriver() *Driver {
	return &Driver{
		typeMap: pgtype.NewMap(),
	}
}

func (d *Driver) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	t, ok := d.typeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("get pg type %s: %w", name, ErrorTypeNotFound)
	}
	return d.EncodeValueByTypeOid(t.OID, src, buf)
}

func (d *Driver) EncodeValueByTypeOid(oid uint32, src any, buf []byte) ([]byte, error) {
	return d.typeMap.Encode(oid, pgtype.TextFormatCode, src, buf)
}

func (d *Driver) DecodeValueByTypeName(name string, src []byte) (any, error) {
	t, ok := d.typeMap.TypeForName(name)
	if !ok {
		return nil, fmt.Errorf("get pg type %s: %w", name, ErrorTypeNotFound)
	}
	return t.Codec.DecodeValue(d.typeMap, t.OID, pgtype.TextFormatCode, src)
}

func (d *Driver) DecodeValueByTypeOid(oid uint32, src []byte) (any, error) {
	t, ok := d.typeMap.TypeForOID(oid)
	if !ok {
		return nil, fmt.Errorf("get pg type %d: %w", oid, ErrorTypeNotFound)
	}
	return t.Codec.DecodeValue(d.typeMap, oid, pgtype.TextFormatCode, src)
}

func (d *Driver) ScanValueByTypeName(name string, src []byte, dest any) error {
	t, ok := d.typeMap.TypeForName(name)
	if !ok {
		return fmt.Errorf("get pg type %s: %w", name, ErrorTypeNotFound)
	}
	return d.typeMap.Scan(t.OID, pgtype.TextFormatCode, src, dest)
}

func (d *Driver) ScanValueByTypeOid(oid uint32, src []byte, dest any) error {
	t, ok := d.typeMap.TypeForOID(oid)
	if !ok {
		return fmt.Errorf("get pg type %d: %w", oid, ErrorTypeNotFound)
	}
	return d.typeMap.Scan(t.OID, pgtype.TextFormatCode, src, dest)
}

func (d *Driver) TypeExistsByName(name string) bool {
	_, ok := d.typeMap.TypeForName(name)
	return ok
}

func (d *Driver) TypeExistsByOid(oid uint32) bool {
	_, ok := d.typeMap.TypeForOID(oid)
	return ok
}

func (d *Driver) GetTypeOid(name string) (uint32, error) {
	t, ok := d.typeMap.TypeForName(name)
	if !ok {
		return 0, fmt.Errorf("get pg type %s: %w", name, ErrorTypeNotFound)
	}
	return t.OID, nil
}
