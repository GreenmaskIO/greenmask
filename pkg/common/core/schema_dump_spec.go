package core

type SchemaDumpKind string

const (
	SchemaDumpKindMySQLPreData  SchemaDumpKind = "mysql.predata"
	SchemaDumpKindMySQLPostData SchemaDumpKind = "mysql.podstdata"

	SchemaDumpKindPostgresSchema SchemaDumpKind = "pg.schema"
)

type SchemaDumpSpec struct {
	TaskID       TaskID
	Kind         ObjectKind
	Name         string
	NeedDumpData bool
	// Payload contains fully resolved object-specific runtime context
	// required for dump object initialization.
	//
	// Examples:
	//   - TableDumpContextPayload
	//   - SequenceDumpContextPayload
	//   - LargeObjectDumpContextPayload
	//
	// Payload is produced during DumpContext building phase and later
	// consumed by DumpObjectFactory to initialize executable Dumper objects.
	Payload any
}
