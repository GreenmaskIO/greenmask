// Copyright 2023 Greenmask
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

package toolkit

import (
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
)

var (
	KindOfType = map[rune]string{
		'b': "Base",
		'c': "Composite",
		'd': "Domain",
		'e': "Enum",
		'p': "PreSudo",
		'r': "Range",
		'm': "Multirange",
	}
)

// Type - describes pg_catalog.pg_type
type Type struct {
	// Oid - pg_type.oid
	Oid Oid `json:"oid,omitempty"`
	// ChainOids - list of inherited types (oid) till the main base type
	ChainOids []Oid `json:"chain_oids,omitempty"`
	// ChainNames - list of inherited types (name) till the main base type
	ChainNames []string `json:"chain_names,omitempty"`
	// Schema - type schema name
	Schema string `json:"schema,omitempty"`
	// Name - (pg_type.typname) type name
	Name string `json:"name,omitempty"`
	// Length - (pg_type.typelen) for a fixed-size type, typlen is the number of bytes in the internal representation of the type.
	// But for a variable-length type, typlen is negative. -1 indicates a “varlena” type (one that has a length
	// word), -2 indicates a null-terminated C string.
	Length int `json:"length,omitempty"`
	// Kind - (pg_type.typtype) type of type
	Kind rune `json:"kind,omitempty"`
	// ComposedRelation - (pg_type.typrelid) if composite type reference to the table that defines the structure
	ComposedRelation Oid `json:"composed_relation,omitempty"`
	// ElementType - (pg_type.typelem) references to the item of the array type
	ElementType Oid `json:"element_type,omitempty"`
	// ArrayType - (pg_type.typarray) references to the array type
	ArrayType Oid `json:"array_type,omitempty"`
	// NotNull - (pg_type.typnotnull) shows is this type nullable. For domains only
	NotNull bool `json:"not_null,omitempty"`
	// BaseType - (pg_type.typbasetype) references to the base type
	BaseType Oid `json:"base_type,omitempty"`
	//Check - definition of check constraint
	Check *Check `json:"check,omitempty"`
	// RootBuiltInTypeOid - defines builtin type oid that might be used for decoding and encoding
	RootBuiltInTypeOid Oid `json:"root_built_in_type_oid,omitempty"`
	// RootBuiltInTypeOid - defines builtin type name that might be used for decoding and encoding
	RootBuiltInTypeName string `json:"root_built_in_type_name,omitempty"`
}

func (t *Type) IsAffected(p *StaticParameter) (w ValidationWarnings) {
	if p.Column == nil {
		panic("parameter Column must not be nil")
	}
	if p.GetDefinition().ColumnProperties == nil {
		panic("parameter ColumnProperties must not be nil")
	}
	if !p.GetDefinition().ColumnProperties.Affected {
		return
	}
	if p.Column.TypeOid != t.Oid {
		return
	}
	if p.GetDefinition().ColumnProperties.Nullable && p.GetDefinition().Column.NotNull {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ParameterName", p.GetDefinition().Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeName", p.GetDefinition().Name).
			SetMsg("transformer may produce NULL values but column type has NOT NULL constraint"),
		)
	}
	if t.Check != nil {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			AddMeta("ParameterName", p.GetDefinition().Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeSchema", t.Schema).
			AddMeta("TypeName", t.Name).
			AddMeta("TypeConstraintSchema", t.Check.Schema).
			AddMeta("TypeConstraintName", t.Check.Schema).
			AddMeta("TypeConstraintDef", t.Check.Definition).
			SetMsg("possible check constraint violation: column has domain type with constraint"),
		)
	}
	if t.Length != WithoutMaxLength && t.Length < p.GetDefinition().ColumnProperties.MaxLength {
		w = append(w, NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			SetMsg("transformer value might be out of length range: domain has length higher than column").
			AddMeta("ParameterName", p.GetDefinition().Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeSchema", t.Schema).
			AddMeta("TypeName", t.Name).
			AddMeta("TypeLength", t.Length).
			AddMeta("ColumnLength", p.Column.Length),
		)
	}
	return
}

func TryRegisterCustomTypes(typeMap *pgtype.Map, types []*Type, silent bool) {
	for _, t := range types {
		// Test is this type already registered
		_, ok := typeMap.TypeForOID(uint32(t.Oid))
		if ok {
			continue
		}
		if t.Kind == 'd' {
			if t.BaseType != 0 {
				baseType, ok := typeMap.TypeForOID(uint32(t.BaseType))
				if !ok && !silent {
					log.Warn().
						Str("Context", "CustomTypeRegistering").
						Str("Schema", t.Schema).
						Str("Name", t.Name).
						Int("Oid", int(t.Oid)).
						Str("Kind", fmt.Sprintf("%c", t.Kind)).
						Msg("unable to register domain type")
					continue
				}
				typeMap.RegisterType(&pgtype.Type{
					Name:  t.Name,
					OID:   uint32(t.Oid),
					Codec: baseType.Codec,
				})
				arrayType, ok := typeMap.TypeForName(fmt.Sprintf("_%s", baseType.Name))
				if !ok && !silent {
					log.Warn().
						Str("Context", "CustomTypeRegistering").
						Str("Schema", t.Schema).
						Str("Name", t.Name).
						Int("Oid", int(t.Oid)).
						Msg("cannot register array type for custom type")
					continue
				}
				arrayTypeName := fmt.Sprintf("_%s", t.Name)
				typeMap.RegisterType(&pgtype.Type{
					Name:  arrayTypeName,
					OID:   uint32(t.ArrayType),
					Codec: arrayType.Codec,
				})
			}
		}
	}
}

func IsTypeAllowedWithTypeMap(
	driver *Driver, allowedTypes []string, typeName string, typeOid Oid, checkInherited bool,
) bool {
	// Get canonical type name by type Oid if exists otherwise use provided name
	pgType, ok := driver.GetTypeMap().TypeForOID(uint32(typeOid))
	if ok {
		typeName = pgType.Name
	}
	return IsTypeAllowed(driver, allowedTypes, typeName, checkInherited)
}

func IsTypeAllowed(
	driver *Driver, allowedTypes []string, typeName string, checkInherited bool,
) bool {

	if slices.Contains(allowedTypes, typeName) {
		return true
	}

	if !checkInherited {
		return false
	}

	// If custom type is found check that the root type is allowed
	pgCustomRootType := GetCustomType(driver.CustomTypes, typeName)
	if pgCustomRootType == nil {
		return false
	}

	for _, t := range pgCustomRootType.ChainNames {
		if slices.Contains(allowedTypes, t) {
			return true
		}
	}

	return false
}

func IsTypeCustom(customTypes []*Type, typeOid Oid) bool {
	return slices.ContainsFunc(customTypes, func(t *Type) bool {
		return t.Oid == typeOid
	})
}

func GetCustomType(customTypes []*Type, typeName string) *Type {
	idx := slices.IndexFunc(customTypes, func(t *Type) bool {
		return t.Name == typeName
	})
	if idx == -1 {
		return nil
	}

	return customTypes[idx]
}

func AreTypesHaveEqualOrHaveEqualBaseTypes(driver *Driver, customTypes []*Type, a string, b string) bool {
	// check type a and b are custom
	if a == b {
		return true
	}

	pgCustomRootTypeA := GetCustomType(customTypes, a)
	if pgCustomRootTypeA == nil {
		return false
	}

	pgCustomRootTypeB := GetCustomType(customTypes, b)
	if pgCustomRootTypeB == nil {
		return false
	}

	// Check chain
	for _, chainItemA := range pgCustomRootTypeA.ChainNames {
		for _, chainItemB := range pgCustomRootTypeB.ChainNames {
			if chainItemA == chainItemB {
				return true
			}
		}
	}

	return false
}
