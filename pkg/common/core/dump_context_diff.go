package core

type DumpContextDiffInput struct {
	Previous *DumpContextSnapshot
	Current  DumpContextSnapshot
}

// DumpContextDiff is the result of comparing two DumpContextSnapshot instances.
//
// The structure is intentionally open-ended — concrete diff semantics
// (added/removed/changed objects, transformation drift, subset changes, etc.)
// will be defined when the differ implementation is built.
type DumpContextDiff struct {
	Previous *DumpContextSnapshot
	Current  DumpContextSnapshot
	// SchemaVersionChanged is true when Previous and Current were produced by
	// different snapshot schema versions. The differ should suppress false-positive
	// drift on fields that did not exist in the older schema version.
	SchemaVersionChanged bool
}
