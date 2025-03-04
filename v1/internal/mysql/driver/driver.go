package driver

import (
	"fmt"
	"strconv"
	"time"
)

type Driver struct {
	// con - dummy connection to mysql that used the internal to encode values
	loc *time.Location
}

func NewDriver() *Driver {
	return &Driver{
		loc: time.Now().Location(),
	}
}

func (e *Driver) EncodeValueByTypeOid(oid uint32, src any, buf []byte) ([]byte, error) {
	typeName, ok := VirtualOidToTypeName[oid]
	if !ok {
		return nil, fmt.Errorf("unsupported oid %d", oid)
	}
	return e.EncodeValueByTypeName(typeName, src, buf)
}

func (e *Driver) DecodeValueByTypeOid(oid uint32, src []byte) (any, error) {
	typeName, ok := VirtualOidToTypeName[oid]
	if !ok {
		return nil, fmt.Errorf("unsupported oid %d", oid)
	}
	return e.DecodeValueByTypeName(typeName, src)
}

func (e *Driver) ScanValueByTypeOid(oid uint32, src []byte, dest any) error {
	typeName, ok := VirtualOidToTypeName[oid]
	if !ok {
		return fmt.Errorf("unsupported oid %d", oid)
	}
	return e.ScanValueByTypeName(typeName, src, dest)
}

func (e *Driver) TypeExistsByName(name string) bool {
	_, ok := TypeNameToVirtualOid[name]
	return ok
}

func (e *Driver) TypeExistsByOid(oid uint32) bool {
	_, ok := VirtualOidToTypeName[oid]
	return ok
}

func (e *Driver) GetTypeOid(name string) (uint32, error) {
	oid, ok := TypeNameToVirtualOid[name]
	if !ok {
		return 0, fmt.Errorf("unsupported type %s", name)
	}
	return oid, nil
}

func (e *Driver) WithLocation(loc *time.Location) *Driver {
	e.loc = loc
	return e
}

func (e *Driver) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	switch name {
	case TypeJSON:
		return encodeJson(src, buf)
	case TypeTimestamp,
		TypeDateTime,
		TypeDate:
		return encodeTimestamp(src, buf, e.loc)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeTime, TypeYear:
		return encodeInt64(src, buf)
	case TypeFloat, TypeDouble, TypeReal:
		return encodeFloat(src, buf)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return encodeString(src, buf)
	case TypeBoolean:
		return encodeBool(src, buf)
	case TypeBinary, TypeVarBinary, TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob:
		return encodeBinary(src, buf)
	case TypeEnum, TypeSet:
		return encodeEnum(src, buf)
	case TypeGeometry, TypePoint, TypeLineString, TypePolygon, TypeMultiPoint, TypeMultiLineString,
		TypeMultiPolygon, TypeGeometryCollection:
		return encodeGeometry(src, buf)
	case TypeDecimal, TypeNumeric:
		return encodeDecimal(src, buf)
	}
	return nil, fmt.Errorf("unsupported type %s", name)
}

func (e *Driver) DecodeValueByTypeName(name string, src []byte) (any, error) {
	// Consider opts pattern usage
	switch name {
	case TypeJSON:
		return string(src), nil
	case TypeTimestamp,
		TypeDateTime,
		TypeDate:
		return parseDateTime(src, e.loc)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeTime, TypeYear:
		// Here may be unsigned type consider to add it but it is likely redundant
		return strconv.ParseInt(string(src), 10, 64)
	case TypeFloat, TypeDouble, TypeReal:
		return strconv.ParseFloat(string(src), 64)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return string(src), nil
	case TypeBoolean:
		return decodeBool(src)
	case TypeBinary, TypeVarBinary, TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob:
		// I suspect there might be some hex encoding
		return src, nil
	case TypeEnum, TypeSet:
		return decodeEnum(src)
	case TypeGeometry, TypePoint, TypeLineString, TypePolygon, TypeMultiPoint, TypeMultiLineString,
		TypeMultiPolygon, TypeGeometryCollection:
		return decodeGeometry(src)
	case TypeDecimal, TypeNumeric:
		return decodeDecimal(src)
	}
	return nil, fmt.Errorf("unsupported type %s", name)
}

func (e *Driver) ScanValueByTypeName(name string, src []byte, dest any) error {
	switch name {
	case TypeJSON:
		return scanJson(src, dest)
	case TypeTimestamp,
		TypeDateTime,
		TypeDate:
		return scanTimestamp(src, dest, e.loc)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeTime, TypeYear:
		return scanInt64(src, dest)
	case TypeFloat, TypeDouble, TypeReal:
		return scanFloat(src, dest)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return scanString(src, dest)
	case TypeBoolean:
		return scanBool(src, dest)
	case TypeBinary, TypeVarBinary, TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob:
		// I suspect there might be some hex encoding
		return scanBinary(src, dest)
	case TypeEnum, TypeSet:
		return scanEnum(src, dest)
	case TypeGeometry, TypePoint, TypeLineString, TypePolygon, TypeMultiPoint, TypeMultiLineString,
		TypeMultiPolygon, TypeGeometryCollection:
		return scanGeometry(src, dest)
	case TypeDecimal, TypeNumeric:
		return scanDecimal(src, dest)
	}
	return fmt.Errorf("unsupported type %s", name)
}
