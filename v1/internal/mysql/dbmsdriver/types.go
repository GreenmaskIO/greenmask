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

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

var NullValueSeq = []byte("\\N")

const (
	// Numeric types
	TypeTinyInt   = "tinyint"
	TypeSmallInt  = "smallint"
	TypeMediumInt = "mediumint"
	TypeInt       = "int"
	TypeBigInt    = "bigint"
	TypeDecimal   = "decimal"
	TypeNumeric   = "numeric"
	TypeFloat     = "float"
	TypeDouble    = "double"
	TypeReal      = "real"
	TypeBit       = "bit"

	// Date and time types
	TypeDate      = "date"
	TypeDateTime  = "datetime"
	TypeTimestamp = "timestamp"
	TypeTime      = "time"
	TypeYear      = "year"

	// String types
	TypeChar    = "char"
	TypeVarChar = "varchar"

	TypeBoolean = "boolean"

	// Text types
	TypeTinyText   = "tinytext"
	TypeText       = "text"
	TypeMediumText = "mediumtext"
	TypeLongText   = "longtext"

	// Binary types
	TypeBinary    = "binary"
	TypeVarBinary = "varbinary"

	// Blob types
	TypeTinyBlob   = "tinyblob"
	TypeBlob       = "blob"
	TypeMediumBlob = "mediumblob"
	TypeLongBlob   = "longblob"

	// Special string types
	TypeEnum = "enum"
	TypeSet  = "set"

	// Spatial types
	TypeGeometry           = "geometry"
	TypePoint              = "point"
	TypeLineString         = "linestring"
	TypePolygon            = "polygon"
	TypeMultiPoint         = "multipoint"
	TypeMultiLineString    = "multilinestring"
	TypeMultiPolygon       = "multipolygon"
	TypeGeometryCollection = "geometrycollection"

	// JSON type
	TypeJSON = "json"
)

const (
	// Numeric types with Virtual OIDs
	VirtualOidTinyInt models.VirtualOID = iota
	VirtualOidSmallInt
	VirtualOidMediumInt
	VirtualOidInt
	VirtualOidBigInt
	VirtualOidDecimal
	VirtualOidNumeric
	VirtualOidFloat
	VirtualOidDouble
	VirtualOidReal
	VirtualOidBit

	// Date and time types
	VirtualOidDate
	VirtualOidDateTime
	VirtualOidTimestamp
	VirtualOidTime
	VirtualOidYear

	// String types
	VirtualOidChar
	VirtualOidVarChar

	VirtualOidBoolean

	// Text types
	VirtualOidTinyText
	VirtualOidText
	VirtualOidMediumText
	VirtualOidLongText

	// Binary types
	VirtualOidBinary
	VirtualOidVarBinary

	// Blob types
	VirtualOidTinyBlob
	VirtualOidBlob
	VirtualOidMediumBlob
	VirtualOidLongBlob

	// Special string types
	VirtualOidEnum
	VirtualOidSet

	// Spatial types
	VirtualOidGeometry
	VirtualOidPoint
	VirtualOidLineString
	VirtualOidPolygon
	VirtualOidMultiPoint
	VirtualOidMultiLineString
	VirtualOidMultiPolygon
	VirtualOidGeometryCollection

	// JSON type
	VirtualOidJSON
)

var (
	VirtualOidToTypeName = map[models.VirtualOID]string{
		VirtualOidTinyInt:            TypeTinyInt,
		VirtualOidSmallInt:           TypeSmallInt,
		VirtualOidMediumInt:          TypeMediumInt,
		VirtualOidInt:                TypeInt,
		VirtualOidBigInt:             TypeBigInt,
		VirtualOidDecimal:            TypeDecimal,
		VirtualOidNumeric:            TypeNumeric,
		VirtualOidFloat:              TypeFloat,
		VirtualOidDouble:             TypeDouble,
		VirtualOidReal:               TypeReal,
		VirtualOidBit:                TypeBit,
		VirtualOidDate:               TypeDate,
		VirtualOidDateTime:           TypeDateTime,
		VirtualOidTimestamp:          TypeTimestamp,
		VirtualOidTime:               TypeTime,
		VirtualOidYear:               TypeYear,
		VirtualOidChar:               TypeChar,
		VirtualOidVarChar:            TypeVarChar,
		VirtualOidBoolean:            TypeBoolean,
		VirtualOidTinyText:           TypeTinyText,
		VirtualOidText:               TypeText,
		VirtualOidMediumText:         TypeMediumText,
		VirtualOidLongText:           TypeLongText,
		VirtualOidBinary:             TypeBinary,
		VirtualOidVarBinary:          TypeVarBinary,
		VirtualOidTinyBlob:           TypeTinyBlob,
		VirtualOidBlob:               TypeBlob,
		VirtualOidMediumBlob:         TypeMediumBlob,
		VirtualOidLongBlob:           TypeLongBlob,
		VirtualOidEnum:               TypeEnum,
		VirtualOidSet:                TypeSet,
		VirtualOidGeometry:           TypeGeometry,
		VirtualOidPoint:              TypePoint,
		VirtualOidLineString:         TypeLineString,
		VirtualOidPolygon:            TypePolygon,
		VirtualOidMultiPoint:         TypeMultiPoint,
		VirtualOidMultiLineString:    TypeMultiLineString,
		VirtualOidMultiPolygon:       TypeMultiPolygon,
		VirtualOidGeometryCollection: TypeGeometryCollection,
		VirtualOidJSON:               TypeJSON,
	}

	TypeNameToVirtualOid = map[string]models.VirtualOID{
		TypeTinyInt:            VirtualOidTinyInt,
		TypeSmallInt:           VirtualOidSmallInt,
		TypeMediumInt:          VirtualOidMediumInt,
		TypeInt:                VirtualOidInt,
		TypeBigInt:             VirtualOidBigInt,
		TypeDecimal:            VirtualOidDecimal,
		TypeNumeric:            VirtualOidNumeric,
		TypeFloat:              VirtualOidFloat,
		TypeDouble:             VirtualOidDouble,
		TypeReal:               VirtualOidReal,
		TypeBit:                VirtualOidBit,
		TypeDate:               VirtualOidDate,
		TypeDateTime:           VirtualOidDateTime,
		TypeTimestamp:          VirtualOidTimestamp,
		TypeTime:               VirtualOidTime,
		TypeYear:               VirtualOidYear,
		TypeChar:               VirtualOidChar,
		TypeVarChar:            VirtualOidVarChar,
		TypeBoolean:            VirtualOidBoolean,
		TypeTinyText:           VirtualOidTinyText,
		TypeText:               VirtualOidText,
		TypeMediumText:         VirtualOidMediumText,
		TypeLongText:           VirtualOidLongText,
		TypeBinary:             VirtualOidBinary,
		TypeVarBinary:          VirtualOidVarBinary,
		TypeTinyBlob:           VirtualOidTinyBlob,
		TypeBlob:               VirtualOidBlob,
		TypeMediumBlob:         VirtualOidMediumBlob,
		TypeLongBlob:           VirtualOidLongBlob,
		TypeEnum:               VirtualOidEnum,
		TypeSet:                VirtualOidSet,
		TypeGeometry:           VirtualOidGeometry,
		TypePoint:              VirtualOidPoint,
		TypeLineString:         VirtualOidLineString,
		TypePolygon:            VirtualOidPolygon,
		TypeMultiPoint:         VirtualOidMultiPoint,
		TypeMultiLineString:    VirtualOidMultiLineString,
		TypeMultiPolygon:       VirtualOidMultiPolygon,
		TypeGeometryCollection: VirtualOidGeometryCollection,
		TypeJSON:               VirtualOidJSON,
	}
)
