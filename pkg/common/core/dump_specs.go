package core

// DumpContext holds the two spec lists produced by the DumpContext building
// phase and consumed by the dump processor.
//
// The split between ObjectDumpSpecs and SchemaDumpSpecs is intentional.
// Schema specs drive a vendor CLI tool (mysqldump, pg_dump) that owns the
// entire DDL of a database or schema in one shot. Object specs drive
// greenmask's own transformer pipeline, which operates row-by-row on
// individual objects (tables, sequences, large objects). Merging them into a
// single generic list — as pg_dump does in its TOC — would push the
// schema-vs-data discrimination from the type system into runtime switches
// throughout the processor and factory layers, gaining nothing in return.
// Moreover, greenmask explicitly separates the dump into two sequential
// stages — schema dump first, data dump second — and the two-list model
// maps directly onto those stages, making that boundary visible in the type
// system rather than implicit in ordering.
// Cross-type dependency ordering, if ever needed, should be expressed
// explicitly in DumpPlan rather than by collapsing this distinction.
type DumpContext struct {
	DumpObjectSpecs []ObjectDumpSpec
	SchemaDumpSpecs []SchemaDumpSpec
	// Source describes the dump source (identity + engine). It is populated by
	// the context builders so later stages (snapshot, plan) don't re-derive it
	// from the per-object specs.
	Source SourceSpec
}

// SourceSpec describes the dump source at the DumpContext level so that later
// stages can reference it without gathering it from the individual object specs.
type SourceSpec struct {
	// Identity is the stable identity of the dump source (engine + scope, e.g.
	// the set of databases). Used directly as the snapshot source identity/key.
	Identity EntityIdentity
	// Engine identifies the DBMS engine of the source.
	Engine DBMSEngine
	// Payload carries the engine-specific source description for later stages.
	//
	// Examples:
	//   - MySQLSourceDatabasePayload
	Payload any
}

type DumpMode string

const (
	DumpModeRaw         DumpMode = "raw"
	DumpModeTransformed DumpMode = "transformed"
)

type ObjectDumpSpec struct {
	TaskID   TaskID
	Kind     ObjectKind
	ObjectID ObjectID
	Name     string
	// Identity is the stable, engine-agnostic identity of the object (kind +
	// name parts). Populated by the context builders so later stages (snapshot,
	// plan) can reference the object without reconstructing it from the payload.
	Identity EntityIdentity
	// Origin records which context builder set the object: the explicit builder
	// (user configuration) or the derived builder (e.g. an inherited primary-key
	// transformation).
	Origin ObjectOrigin
	Mode   DumpMode
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

// SchemaObjectKind identifies a schema-holding object (a database/schema) whose
// DDL is dumped by a vendor utility (mysqldump, pg_dump) rather than by greenmask
// itself. There is one kind per engine: the vendor dumper is selected by this
// kind, and the pre-data/post-data split is carried inside the Payload.
//
// It is a distinct namespace from ObjectKind (the kinds of objects greenmask
// dumps itself, e.g. tables), but both reference introspected objects through
// the shared runtime ObjectID.
//
// Concrete kinds are owned by the engine packages (e.g. pg.database,
// mysql.database); core defines only the open string type and never enumerates
// any engine's schema kinds.
type SchemaObjectKind string

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
