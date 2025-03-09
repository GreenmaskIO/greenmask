package domains

// Reference - represents a foreign key constraint.
type Reference struct {
	// ReferencedSchema - schema of the referenced table.
	ReferencedSchema string
	// ReferencedName - name of the referenced table.
	ReferencedName string
	// ConstraintName - name of the foreign key constraint.
	ConstraintName string
	// ConstraintSchema - schema of the foreign key constraint.
	ConstraintSchema string
	// ReferencedKeys - list of the keys that involved in the foreign key constraint.
	Keys []string
	// IsNullable - flag that indicates whether the foreign key constraint is nullable.
	IsNullable bool
}

func NewReference(
	referencedSchema, referencedName, constraintSchema, constraintName string,
	keys []string,
	isNullable bool,
) Reference {
	return Reference{
		ReferencedSchema: referencedSchema,
		ReferencedName:   referencedName,
		ConstraintSchema: constraintSchema,
		ConstraintName:   constraintName,
		Keys:             keys,
		IsNullable:       isNullable,
	}
}

func (r *Reference) SetKeys(keys []string) {
	r.Keys = keys
}
