package core

// SchemaObjectKind identifies a schema-holding object (a database/schema) whose
// DDL is dumped by a vendor utility (mysqldump, pg_dump) rather than by greenmask
// itself. There is one kind per engine: the vendor dumper is selected by this
// kind, and the pre-data/post-data split is carried separately by
// SchemaDumpSpec.Section.
//
// It is a distinct namespace from ObjectKind (the kinds of objects greenmask
// dumps itself, e.g. tables), but both reference introspected objects through
// the shared runtime ObjectID.
type SchemaObjectKind string

const (
	// SchemaObjectKindMysqlDatabase — MySQL schema delegated to mysqldump.
	SchemaObjectKindMysqlDatabase SchemaObjectKind = "mysql.database"
	// SchemaObjectKindPostgresSchema — PostgreSQL schema delegated to pg_dump.
	SchemaObjectKindPostgresSchema SchemaObjectKind = "pg.schema"
)

type SchemaDumpSpec struct {
	TaskID TaskID
	// Kind selects the vendor schema dumper (one per engine).
	Kind SchemaObjectKind
	// ObjectID is a RUNTIME handle to the introspected schema object. It is valid
	// only within a single run for traversal/correlation (dependency and
	// restoration ordering, stats correlation) and must never be compared or
	// persisted across runs.
	ObjectID ObjectID
	// Name is the database/schema name.
	Name string
	// Section is the dump section (pre-data/post-data) this spec produces. It is a
	// first-class field because a single vendor dumper produces all sections; the
	// section is a parameter of the dump task, not part of its kind.
	Section DumpSection
	// Payload contains fully resolved object-specific runtime context required for
	// dump object initialization. It is kept for forward-compatibility with
	// engines that need richer context; for MySQL it is nil because Name and
	// Section fully describe the task.
	Payload any
}
