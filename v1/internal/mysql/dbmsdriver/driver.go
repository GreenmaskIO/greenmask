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

	"github.com/rs/zerolog/log"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

var (
	_ commonininterfaces.DBMSDriver = (*Driver)(nil)
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

func (e *Driver) EncodeValueByTypeOid(oid commonmodels.VirtualOID, src any, buf []byte) ([]byte, error) {
	typeName, ok := VirtualOidToTypeName[oid]
	if !ok {
		return nil, fmt.Errorf("unsupported oid %d", oid)
	}
	return e.EncodeValueByTypeName(typeName, src, buf)
}

func (e *Driver) DecodeValueByTypeOid(oid commonmodels.VirtualOID, src []byte) (any, error) {
	typeName, ok := VirtualOidToTypeName[oid]
	if !ok {
		return nil, fmt.Errorf("unsupported oid %d", oid)
	}
	return e.DecodeValueByTypeName(typeName, src)
}

func (e *Driver) ScanValueByTypeOid(oid commonmodels.VirtualOID, src []byte, dest any) error {
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

func (e *Driver) TypeExistsByOid(oid commonmodels.VirtualOID) bool {
	_, ok := VirtualOidToTypeName[oid]
	return ok
}

func (e *Driver) GetTypeOid(name string) (commonmodels.VirtualOID, error) {
	oid, ok := TypeNameToVirtualOid[name]
	if !ok {
		return 0, fmt.Errorf("unsupported type %s", name)
	}
	return oid, nil
}

func (e *Driver) GetCanonicalTypeName(name string, oid commonmodels.VirtualOID) (string, error) {
	// TODO: implement canonical dbmsdriver.Driver.GetCanonicalTypeName
	log.Warn().Msg("implement canonical dbmsdriver.Driver.GetCanonicalTypeName")
	return name, nil
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
	case TypeBit:
		return encodeBit(src, buf)
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
	case TypeTime:
		return decodeTime(src)
	case TypeTinyInt, TypeSmallInt, TypeMediumInt, TypeInt, TypeBigInt, TypeYear:
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
	case TypeBit:
		return scanBit(src, dest)
	}
	return fmt.Errorf("unsupported type %s", name)
}
