package core

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
	Mode     DumpMode
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
