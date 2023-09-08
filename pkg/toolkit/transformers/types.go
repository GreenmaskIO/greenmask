package transformers

import "github.com/greenmaskio/greenmask/internal/domains"

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
	Oid Oid
	// Schema - type schema name
	Schema string
	// Name - (pg_type.typname) type name
	Name string
	// Length - (pg_type.typelen) for a fixed-size type, typlen is the number of bytes in the internal representation of the type.
	// But for a variable-length type, typlen is negative. -1 indicates a “varlena” type (one that has a length
	// word), -2 indicates a null-terminated C string.
	Length int
	// Kind - (pg_type.typtype) type of type
	Kind rune
	// ComposedRelation - (pg_type.typrelid) if composite type reference to the table that defines the structure
	ComposedRelation Oid
	// ElementType - (pg_type.typelem) references to the item of the array type
	ElementType Oid
	// ArrayType - (pg_type.typarray) references to the array type
	ArrayType Oid
	// NotNull - (pg_type.typnotnull) shows is this type nullable. For domains only
	NotNull bool
	// BaseType - (pg_type.typbasetype) references to the base type
	BaseType Oid
	//Check - definition of check constraint
	Check *Check
}

func (t *Type) IsAffected(p *Parameter) (w ValidationWarnings) {
	if p.Column == nil {
		panic("parameter Column must not be nil")
	}
	if p.Column == nil {
		panic("parameter ColumnProperties must not be nil")
	}
	if !p.ColumnProperties.Affected {
		return
	}
	if p.Column.TypeOid != t.Oid {
		return
	}
	if p.ColumnProperties.Nullable && p.Column.NotNull {
		w = append(w, NewValidationWarning().
			SetLevel(WarningValidationSeverity).
			AddMeta("ParameterName", p.Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeName", p.Name).
			SetMsg("transformer may produce NULL values but column type has NOT NULL constraint"),
		)
	}
	if t.Check != nil {
		w = append(w, NewValidationWarning().
			SetLevel(WarningValidationSeverity).
			AddMeta("ParameterName", p.Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeSchema", t.Schema).
			AddMeta("TypeName", t.Name).
			AddMeta("TypeConstraintSchema", t.Check.Schema).
			AddMeta("TypeConstraintName", t.Check.Schema).
			AddMeta("TypeConstraintDef", t.Check.Definition).
			SetMsg("possible check constraint violation: column has domain type with constraint"),
		)
	}
	if t.Length != WithoutMaxLength && t.Length < p.ColumnProperties.MaxLength {
		w = append(w, NewValidationWarning().
			SetLevel(domains.WarningValidationSeverity).
			SetMsg("transformer value might be out of length range: domain has length higher than column").
			AddMeta("ParameterName", p.Name).
			AddMeta("ColumnName", p.Column.Name).
			AddMeta("TypeSchema", t.Schema).
			AddMeta("TypeName", t.Name).
			AddMeta("TypeLength", t.Length).
			AddMeta("ColumnLength", p.Column.Length),
		)
	}
	return
}
