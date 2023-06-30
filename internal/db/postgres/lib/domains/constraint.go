package domains

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

type Constraint struct {
	// Oid - constraint oid pg_constraint.oid
	Oid Oid `json:"-" yaml:"-"`
	// Name - constraint name
	Name string `json:"-" yaml:"-"`
	// Schema - constraint schema name
	Schema string `json:"-" yaml:"-"`
	// Def - constraint definition
	Def string `json:"-" yaml:"-"`
	// Type - type of the constraint. Possible values: c = check constraint, f = foreign key constraint,
	//  	  p = primary key constraint, u = unique constraint, t = constraint trigger, x = exclusion constraint
	Type string `json:"-" yaml:"-"`
	// Domain - The domain this constraint is on; zero if not a domain constraint
	Domain Oid `json:"-" yaml:"-"`
	// RootPtConstraint - The corresponding constraint of the parent partitioned table
	RootPtConstraint Oid `json:"-" yaml:"-"`
	// FkTable - references table oid
	FkTable Oid `json:"-" yaml:"-"`
	// ConstrainedColumns - columns at the current table
	ConstrainedColumns []AttNum `json:"-" yaml:"-"`
	// ReferencesColumns - columns at the referenced table only for FK constraints
	ReferencesColumns []AttNum `json:"-" yaml:"-"`
	// ReferencesColumnNums - columns at the referenced table only for FK constraints
	ReferencedTable []Oid `json:"-" yaml:"-"`
}
