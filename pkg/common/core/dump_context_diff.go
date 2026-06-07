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
}
