package core

// SchemaObjectKind identifies a schema-holding object (a database/schema) whose
// DDL is dumped by a vendor utility (mysqldump, pg_dump) rather than by greenmask
// itself. There is one kind per engine: the vendor dumper is selected by this
// kind, and the pre-data/post-data split is carried inside the Payload.
//
// It is a distinct namespace from ObjectKind (the kinds of objects greenmask
// dumps itself, e.g. tables), but both reference introspected objects through
// the shared runtime ObjectID.
type SchemaObjectKind string

const (
	SchemaObjectKindMysqlDatabase    SchemaObjectKind = "mysql.database"
	SchemaObjectKindMysqlSchema      SchemaObjectKind = "mysql.schema"
	SchemaObjectKindPostgresDatabase SchemaObjectKind = "pg.database"
	SchemaObjectKindPostgresSchema   SchemaObjectKind = "pg.schema"
)

// SchemaDumpContextPayload is the base schema-dump context consumed by
// SchemaDumpFactory to initialize a SchemaDumper.
type SchemaDumpContextPayload struct {
	// Name is the database/schema name.
	Name string
	// Section is the dump section (pre-data or post-data).
	Section DumpSection
}

type SchemaDumpSpec struct {
	TaskID TaskID
	// Kind selects the vendor schema dumper (one per engine).
	Kind SchemaObjectKind
	// ObjectID is a RUNTIME handle to the introspected schema object. It is valid
	// only within a single run for traversal/correlation (dependency and
	// restoration ordering, stats correlation) and must never be compared or
	// persisted across runs.
	ObjectID ObjectID
	// Payload contains fully resolved object-specific runtime context
	// required for dump object initialization.
	//
	// Examples:
	//   - SchemaDumpContextPayload
	//
	// Payload is produced during DumpContext building phase and later
	// consumed by SchemaDumpFactory to initialize executable SchemaDumper objects.
	Payload any
}
