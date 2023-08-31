package transformers

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
	// Len - (pg_type.typelen) for a fixed-size type, typlen is the number of bytes in the internal representation of the type.
	// But for a variable-length type, typlen is negative. -1 indicates a “varlena” type (one that has a length
	// word), -2 indicates a null-terminated C string.
	Len int
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
