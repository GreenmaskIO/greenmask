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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var (
	DMMSName = "mysql"

	NullValueSeq = []byte("\\N")
)

const (
	// Numeric types
	TypeTinyInt   = "tinyint"
	TypeSmallInt  = "smallint"
	TypeMediumInt = "mediumint"
	TypeInt       = "int"
	TypeBigInt    = "bigint"

	// Numeric types
	TypeNumeric = "numeric"
	TypeDecimal = "decimal"

	// Floating point types
	TypeFloat  = "float"
	TypeDouble = "double"
	TypeReal   = "real"

	// Date and time types
	TypeDate      = "date"
	TypeDateTime  = "datetime"
	TypeTimestamp = "timestamp"
	TypeTime      = "time"
	TypeYear      = "year"

	// String types
	TypeChar       = "char"
	TypeVarChar    = "varchar"
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

	// Boolean type
	TypeBoolean = "boolean"
	TypeBool    = "bool"
	TypeBit     = "bit"

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
	VirtualOidTinyInt core.VirtualOID = iota
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
	VirtualOidBool

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
	VirtualOidToTypeName = map[core.VirtualOID]string{
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
		VirtualOidBool:               TypeBool,
	}

	TypeNameToVirtualOid = make(map[string]core.VirtualOID)

	// TypeDataNameTypeToClass - mapping MySQL data types to common type classes.
	TypeDataNameTypeToClass = map[string]core.TypeClass{
		TypeChar:       core.TypeClassText,
		TypeVarChar:    core.TypeClassText,
		TypeTinyText:   core.TypeClassText,
		TypeText:       core.TypeClassText,
		TypeMediumText: core.TypeClassText,
		TypeLongText:   core.TypeClassText,

		TypeTinyInt:   core.TypeClassInt,
		TypeSmallInt:  core.TypeClassInt,
		TypeMediumInt: core.TypeClassInt,
		TypeInt:       core.TypeClassInt,
		TypeBigInt:    core.TypeClassInt,

		TypeFloat:  core.TypeClassFloat,
		TypeDouble: core.TypeClassFloat,
		TypeReal:   core.TypeClassFloat,

		TypeNumeric: core.TypeClassFloat,
		TypeDecimal: core.TypeClassFloat,

		TypeBit:     core.TypeClassBoolean,
		TypeBool:    core.TypeClassBoolean,
		TypeBoolean: core.TypeClassBoolean,

		TypeDate:      core.TypeClassDateTime,
		TypeDateTime:  core.TypeClassDateTime,
		TypeTimestamp: core.TypeClassDateTime,
		TypeTime:      core.TypeClassDateTime,

		TypeYear: core.TypeClassTime,

		TypeJSON: core.TypeClassJson,

		TypeBinary:     core.TypeClassBinary,
		TypeVarBinary:  core.TypeClassBinary,
		TypeBlob:       core.TypeClassBinary,
		TypeTinyBlob:   core.TypeClassBinary,
		TypeMediumBlob: core.TypeClassBinary,
		TypeLongBlob:   core.TypeClassBinary,

		TypeEnum: core.TypeClassEnum,
		TypeSet:  core.TypeClassEnum, // MySQL-specific
	}

	TypeDataOidToClass = make(map[core.VirtualOID]core.TypeClass)

	// TypeClassToDataTypes - reverse mapping from common type classes to MySQL data types.
	TypeClassToDataTypes = make(map[core.TypeClass][]string)
)

func init() {
	for oid, typeName := range VirtualOidToTypeName {
		TypeNameToVirtualOid[typeName] = oid
	}

	// Initialize the reverse mapping from type classes to data types.
	for dt, tc := range TypeDataNameTypeToClass {
		TypeClassToDataTypes[tc] = append(TypeClassToDataTypes[tc], dt)
	}

	for dt, tc := range TypeDataNameTypeToClass {
		oid, ok := TypeNameToVirtualOid[dt]
		if !ok {
			panic(fmt.Sprintf("invalid type name \"%s\"", dt))
		}
		TypeDataOidToClass[oid] = tc
	}
}
