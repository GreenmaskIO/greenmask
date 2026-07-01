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

// typeDef is the immutable per-base-type record in the MySQL type catalog. It is
// keyed by the modifier-free base name (DATA_TYPE, e.g. "int") and holds the
// stable facts of that base type. Per-column modifiers — signedness, precision,
// scale, length — are NOT catalog keys; they are overlaid at projection time by
// ResolveType, which keeps the catalog free of the type×sign×precision
// combinatorial blow-up. A class of "" means the base type has no canonical
// class (e.g. spatial types), matching the pre-consolidation behavior where such
// types resolved to TypeClassUnsupported.
type typeDef struct {
	name  string
	id    core.TypeID
	class core.TypeClass
}

// typeDefs is the single source of truth for the MySQL type catalog. All lookup
// maps below are derived from it in init().
var typeDefs = []typeDef{
	{TypeTinyInt, TypeIDTinyInt, core.TypeClassInt},
	{TypeSmallInt, TypeIDSmallInt, core.TypeClassInt},
	{TypeMediumInt, TypeIDMediumInt, core.TypeClassInt},
	{TypeInt, TypeIDInt, core.TypeClassInt},
	{TypeBigInt, TypeIDBigInt, core.TypeClassInt},
	{TypeDecimal, TypeIDDecimal, core.TypeClassFloat},
	{TypeNumeric, TypeIDNumeric, core.TypeClassFloat},
	{TypeFloat, TypeIDFloat, core.TypeClassFloat},
	{TypeDouble, TypeIDDouble, core.TypeClassFloat},
	{TypeReal, TypeIDReal, core.TypeClassFloat},
	{TypeBit, TypeIDBit, core.TypeClassBoolean},
	{TypeDate, TypeIDDate, core.TypeClassDateTime},
	{TypeDateTime, TypeIDDateTime, core.TypeClassDateTime},
	{TypeTimestamp, TypeIDTimestamp, core.TypeClassDateTime},
	{TypeTime, TypeIDTime, core.TypeClassDateTime},
	{TypeYear, TypeIDYear, core.TypeClassTime},
	{TypeChar, TypeIDChar, core.TypeClassText},
	{TypeVarChar, TypeIDVarChar, core.TypeClassText},
	{TypeBoolean, TypeIDBoolean, core.TypeClassBoolean},
	{TypeBool, TypeIDBool, core.TypeClassBoolean},
	{TypeTinyText, TypeIDTinyText, core.TypeClassText},
	{TypeText, TypeIDText, core.TypeClassText},
	{TypeMediumText, TypeIDMediumText, core.TypeClassText},
	{TypeLongText, TypeIDLongText, core.TypeClassText},
	{TypeBinary, TypeIDBinary, core.TypeClassBinary},
	{TypeVarBinary, TypeIDVarBinary, core.TypeClassBinary},
	{TypeTinyBlob, TypeIDTinyBlob, core.TypeClassBinary},
	{TypeBlob, TypeIDBlob, core.TypeClassBinary},
	{TypeMediumBlob, TypeIDMediumBlob, core.TypeClassBinary},
	{TypeLongBlob, TypeIDLongBlob, core.TypeClassBinary},
	{TypeEnum, TypeIDEnum, TypeClassEnum},
	{TypeSet, TypeIDSet, TypeClassEnum}, // MySQL-specific
	{TypeGeometry, TypeIDGeometry, ""},
	{TypePoint, TypeIDPoint, ""},
	{TypeLineString, TypeIDLineString, ""},
	{TypePolygon, TypeIDPolygon, ""},
	{TypeMultiPoint, TypeIDMultiPoint, ""},
	{TypeMultiLineString, TypeIDMultiLineString, ""},
	{TypeMultiPolygon, TypeIDMultiPolygon, ""},
	{TypeGeometryCollection, TypeIDGeometryCollection, ""},
	{TypeJSON, TypeIDJSON, core.TypeClassJson},
}

var (
	// TypeIDToTypeName / TypeNameToTypeID - id<->name lookups for every base type.
	TypeIDToTypeName = make(map[core.TypeID]string, len(typeDefs))
	TypeNameToTypeID = make(map[string]core.TypeID, len(typeDefs))

	// TypeDataNameTypeToClass / TypeDataIDToClass - name/id -> class lookups. Only
	// base types that carry a canonical class are present (matching the historical
	// maps); spatial types with class "" are intentionally absent.
	TypeDataNameTypeToClass = make(map[string]core.TypeClass)
	TypeDataIDToClass       = make(map[core.TypeID]core.TypeClass)

	// TypeClassToDataTypes - reverse mapping from common type classes to MySQL data types.
	TypeClassToDataTypes = make(map[core.TypeClass][]string)
)

func init() {
	for _, td := range typeDefs {
		if _, dup := TypeNameToTypeID[td.name]; dup {
			panic(fmt.Sprintf("duplicate type name %q in typeDefs", td.name))
		}
		TypeIDToTypeName[td.id] = td.name
		TypeNameToTypeID[td.name] = td.id
		if td.class != "" {
			TypeDataNameTypeToClass[td.name] = td.class
			TypeDataIDToClass[td.id] = td.class
			TypeClassToDataTypes[td.class] = append(TypeClassToDataTypes[td.class], td.name)
		}
	}
}
