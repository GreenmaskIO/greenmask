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

// TypeClassEnum is the MySQL-specific type class for ENUM/SET columns. It is an
// engine extension over core's generic type-class set: core deliberately does not
// enumerate single-engine families, so the class lives in the engine driver that
// produces it. Transformers that do not recognize it simply treat the column as
// any other unknown class.
const TypeClassEnum core.TypeClass = "enum"

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
	TypeIDTinyInt core.TypeID = iota
	TypeIDSmallInt
	TypeIDMediumInt
	TypeIDInt
	TypeIDBigInt
	TypeIDDecimal
	TypeIDNumeric
	TypeIDFloat
	TypeIDDouble
	TypeIDReal
	TypeIDBit

	// Date and time types
	TypeIDDate
	TypeIDDateTime
	TypeIDTimestamp
	TypeIDTime
	TypeIDYear

	// String types
	TypeIDChar
	TypeIDVarChar

	TypeIDBoolean
	TypeIDBool

	// Text types
	TypeIDTinyText
	TypeIDText
	TypeIDMediumText
	TypeIDLongText

	// Binary types
	TypeIDBinary
	TypeIDVarBinary

	// Blob types
	TypeIDTinyBlob
	TypeIDBlob
	TypeIDMediumBlob
	TypeIDLongBlob

	// Special string types
	TypeIDEnum
	TypeIDSet

	// Spatial types
	TypeIDGeometry
	TypeIDPoint
	TypeIDLineString
	TypeIDPolygon
	TypeIDMultiPoint
	TypeIDMultiLineString
	TypeIDMultiPolygon
	TypeIDGeometryCollection

	// JSON type
	TypeIDJSON
)

var (
	TypeIDToTypeName = map[core.TypeID]string{
		TypeIDTinyInt:            TypeTinyInt,
		TypeIDSmallInt:           TypeSmallInt,
		TypeIDMediumInt:          TypeMediumInt,
		TypeIDInt:                TypeInt,
		TypeIDBigInt:             TypeBigInt,
		TypeIDDecimal:            TypeDecimal,
		TypeIDNumeric:            TypeNumeric,
		TypeIDFloat:              TypeFloat,
		TypeIDDouble:             TypeDouble,
		TypeIDReal:               TypeReal,
		TypeIDBit:                TypeBit,
		TypeIDDate:               TypeDate,
		TypeIDDateTime:           TypeDateTime,
		TypeIDTimestamp:          TypeTimestamp,
		TypeIDTime:               TypeTime,
		TypeIDYear:               TypeYear,
		TypeIDChar:               TypeChar,
		TypeIDVarChar:            TypeVarChar,
		TypeIDBoolean:            TypeBoolean,
		TypeIDTinyText:           TypeTinyText,
		TypeIDText:               TypeText,
		TypeIDMediumText:         TypeMediumText,
		TypeIDLongText:           TypeLongText,
		TypeIDBinary:             TypeBinary,
		TypeIDVarBinary:          TypeVarBinary,
		TypeIDTinyBlob:           TypeTinyBlob,
		TypeIDBlob:               TypeBlob,
		TypeIDMediumBlob:         TypeMediumBlob,
		TypeIDLongBlob:           TypeLongBlob,
		TypeIDEnum:               TypeEnum,
		TypeIDSet:                TypeSet,
		TypeIDGeometry:           TypeGeometry,
		TypeIDPoint:              TypePoint,
		TypeIDLineString:         TypeLineString,
		TypeIDPolygon:            TypePolygon,
		TypeIDMultiPoint:         TypeMultiPoint,
		TypeIDMultiLineString:    TypeMultiLineString,
		TypeIDMultiPolygon:       TypeMultiPolygon,
		TypeIDGeometryCollection: TypeGeometryCollection,
		TypeIDJSON:               TypeJSON,
		TypeIDBool:               TypeBool,
	}

	TypeNameToTypeID = make(map[string]core.TypeID)

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

		TypeEnum: TypeClassEnum,
		TypeSet:  TypeClassEnum, // MySQL-specific
	}

	TypeDataIDToClass = make(map[core.TypeID]core.TypeClass)

	// TypeClassToDataTypes - reverse mapping from common type classes to MySQL data types.
	TypeClassToDataTypes = make(map[core.TypeClass][]string)
)

func init() {
	for oid, typeName := range TypeIDToTypeName {
		TypeNameToTypeID[typeName] = oid
	}

	// Initialize the reverse mapping from type classes to data types.
	for dt, tc := range TypeDataNameTypeToClass {
		TypeClassToDataTypes[tc] = append(TypeClassToDataTypes[tc], dt)
	}

	for dt, tc := range TypeDataNameTypeToClass {
		oid, ok := TypeNameToTypeID[dt]
		if !ok {
			panic(fmt.Sprintf("invalid type name \"%s\"", dt))
		}
		TypeDataIDToClass[oid] = tc
	}
}
