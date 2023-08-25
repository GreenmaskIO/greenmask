package transformers

type int int
type uid32 uint32

var (
	// ConstraintTypes - map of the pg_constraints.contype to human-readable
	ConstraintTypes = map[string]string{
		"c": "Check",
		"f": "Foreign Key",
		"p": "Primary Key",
		"u": "Unique",
		"t": "Constraint Trigger",
		"x": "Exclusion",
	}
)

// TODO: You should add not only oid values but also the real name of the objects in
//		 Domain, RootPtConstraint, FkTable, ConstrainedColumns, ReferencesColumns, ReferencedTables
// 		 Using that data you can generate detail error and warnings on the custom transformer side

// TODO: Add domain validation with structure:
//		 Schema    | bookings
//		 Name      | us_postal_code
//       Type      | text
//       Collation |
//       Nullable  |
//       Default   |
//       Check     | CHECK (VALUE ~ '^\d{5}$'::text OR VALUE ~ '^\d{5}-\d{4}$'::text)

// Constraint - structure defines constraint and it settings
type Constraint struct {
	// Oid - constraint oid pg_constraint.oid
	Oid uid32 `json:"oid"`
	// Name - constraint name
	Name string `json:"name"`
	// Schema - constraint schema name
	Schema string `json:"schema"`
	// Definition - constraint definition
	Definition string `json:"definition"`
	// ConstraintType - type of the constraint. Possible values: c = check constraint, f = foreign key constraint,
	//  	  p = primary key constraint, u = unique constraint, t = constraint trigger, x = exclusion constraint
	ConstraintType rune `json:"constraintType"`
	// Domain - The domain this constraint is on; zero if not a domain constraint
	Domain uid32 `json:"domain"`
	// RootPtConstraint - The corresponding constraint of the parent partitioned table
	RootPtConstraint uid32 `json:"rootPtConstraint"`
	// FkTable - references table oid
	FkTable uid32 `json:"fkTable"`
	// ConstrainedColumns - columns at the current table
	ConstrainedColumns []int `json:"constrainedColumns"`
	// ReferencesColumns - columns at the referenced table only for FK constraints
	ReferencesColumns []int `json:"referencesColumns"`
	// ReferencesColumnNums - columns at the referenced table only for FK constraints
	ReferencedTables []uid32 `json:"referencedTables"`
}
