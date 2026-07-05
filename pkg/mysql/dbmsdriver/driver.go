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

package dbmsdriver

import (
	"fmt"
	"strconv"
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var (
	_ core.DBMSDriver = (*Driver)(nil)
	// Per-leaf compile-time proofs: the MySQL driver satisfies each type-level
	// leaf DBMSDriver composes.
	_ core.NamedTypeCodec    = (*Driver)(nil)
	_ core.TypedCodec        = (*Driver)(nil)
	_ core.TypeIntrospection = (*Driver)(nil)
)

type Driver struct {
	// con - dummy connection to mysql that used the internal to encode values
	loc *time.Location
}

func New() *Driver {
	return &Driver{
		loc: time.Now().Location(),
	}
}

// typeName resolves the base type name a Type descriptor dispatches on. Dispatch
// is on Name (the authoritative key); only when Name is empty is the base name
// resolved from the type id. A present ID never overrides a present Name — this
// avoids the id-0 footgun (TypeIDTinyInt == 0) that would otherwise mis-resolve a
// name-only descriptor as tinyint.
func (e *Driver) typeName(t core.Type) string {
	if t.Name != "" {
		return t.Name
	}
	if n, ok := TypeIDToTypeName[t.ID]; ok {
		return n
	}
	return t.Name
}

// EncodeValueByType encodes using a full Type descriptor. Encoding is value-driven
// (the Go value carries signedness), so it dispatches on the base name like the
// id/name encoders, just keyed off the self-describing Type.
func (e *Driver) EncodeValueByType(t core.Type, src any, buf []byte) ([]byte, error) {
	return e.EncodeValueByTypeName(e.typeName(t), src, buf)
}

// DecodeValueByType decodes using a full Type descriptor, so signedness (and
// future limits/constraints) drive decoding rather than a bare type id.
func (e *Driver) DecodeValueByType(t core.Type, src []byte) (any, error) {
	return e.decode(e.typeName(t), t.IsSigned(), src)
}

// ScanValueByType scans using a full Type descriptor, dispatching on the base name.
func (e *Driver) ScanValueByType(t core.Type, src []byte, dest any) error {
	return e.ScanValueByTypeName(e.typeName(t), src, dest)
}

func (e *Driver) TypeExistsByName(name string) bool {
	_, ok := TypeNameToTypeID[name]
	return ok
}

func (e *Driver) TypeExistsByID(oid core.TypeID) bool {
	_, ok := TypeIDToTypeName[oid]
	return ok
}

func (e *Driver) GetTypeID(name string) (core.TypeID, error) {
	oid, ok := TypeNameToTypeID[name]
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
	case TypeTime:
		return encodeTime(src, buf)
	case TypeTimestamp,
		TypeDateTime,
		TypeDate:
		return encodeTimestamp(src, buf, e.loc)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeYear:
		return encodeInt64(src, buf)
	case TypeFloat, TypeDouble, TypeReal:
		return encodeFloat(src, buf)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return encodeString(src, buf)
	case TypeBoolean, TypeBool:
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
	case TypeBit:
		return encodeBit(src, buf)
	}
	return nil, fmt.Errorf("unsupported type %s", name)
}

// DecodeValueByTypeName decodes a value by its canonical base type name. It is
// a context-less entry point, so integer types are decoded as signed; callers
// that know a column's signedness must use DecodeValueByType.
func (e *Driver) DecodeValueByTypeName(name string, src []byte) (any, error) {
	return e.decode(name, true, src)
}

// decode is the single type-keyed decode switch shared by every decode entry
// point. Integer types consult the signed flag (the only type-class whose Go
// type depends on a modifier); all other branches are modifier-independent.
func (e *Driver) decode(name string, signed bool, src []byte) (any, error) {
	// Consider opts pattern usage
	switch name {
	case TypeJSON:
		return string(src), nil
	case TypeTimestamp,
		TypeDateTime,
		TypeDate:
		return parseDateTime(src, e.loc)
	case TypeTime:
		return decodeTime(src)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeYear:
		if signed {
			return decodeInt(src)
		}
		return decodeUint(src)
	case TypeFloat, TypeDouble, TypeReal:
		return strconv.ParseFloat(string(src), 64)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return string(src), nil
	case TypeBoolean, TypeBool:
		return decodeBool(src)
	case TypeBinary, TypeVarBinary, TypeTinyBlob, TypeBlob, TypeMediumBlob, TypeLongBlob:
		// I suspect there might be some hex encoding
		return src, nil
	case TypeEnum, TypeSet:
		return decodeEnum(src)
	case TypeGeometry, TypePoint, TypeLineString, TypePolygon, TypeMultiPoint, TypeMultiLineString,
		TypeMultiPolygon, TypeGeometryCollection:
		return src, nil
	case TypeDecimal, TypeNumeric:
		return decodeDecimal(src)
	case TypeBit:
		return decodeBit(src)
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
	case TypeTime:
		return scanTime(src, dest)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeYear:
		return scanInt64(src, dest)
	case TypeFloat, TypeDouble, TypeReal:
		return scanFloat(src, dest)
	case TypeChar, TypeVarChar, TypeTinyText, TypeText, TypeMediumText, TypeLongText:
		return scanString(src, dest)
	case TypeBoolean, TypeBool:
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
	case TypeBit:
		return scanBit(src, dest)
	}
	return fmt.Errorf("unsupported type %s", name)
}

func (e *Driver) GetCanonicalTypeClassName(typeName string, typeOid core.TypeID) (core.TypeClass, error) {
	className, ok := TypeDataNameTypeToClass[typeName]
	if ok {
		return className, nil
	}
	oidClassName, ok := TypeDataIDToClass[typeOid]
	if ok {
		return oidClassName, nil
	}
	return "", fmt.Errorf("find type class \"%s\": %w", typeName, core.ErrUnknownDBMSTypeClass)
}

func (e *Driver) GetCanonicalTypeName(_ string, oid core.TypeID) (string, error) {
	typeName, ok := TypeIDToTypeName[oid]
	if !ok {
		return "", fmt.Errorf("find type \"%s\": %w", typeName, core.ErrUnknownDBMSType)
	}
	return string(typeName), nil
}
