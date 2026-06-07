package core

import "context"

// ObjectFilterInput is passed to ObjectFilter.FilterObjects.
type ObjectFilterInput struct {
	IntrospectionResult IntrospectionResult
	// DumpConfig is the DBMS-specific dump configuration produced by ConfigEditor.
	// It is opaque at the core level; each ObjectFilter implementation asserts it
	// to the concrete type it expects (e.g. []TableConfig for MySQL).
	// It may carry table-level configs as well as higher-level include/exclude
	// options (databases, schemas, patterns).
	DumpConfig any
}

// ObjectFilterResult declares which objects from the introspection are
// allowed to participate in the dump, keyed by kind.
// An empty or nil AllowedObjects map means all objects are allowed.
// Future validation stages can compare this against the full IntrospectionResult
// to detect excluded or missing objects.
type ObjectFilterResult struct {
	AllowedObjects map[ObjectKind][]ObjectID
}

// ObjectFilter sits between ConfigEditor and ExplicitDumpContextBuilder in the
// dump pipeline. It receives the full introspection and the DBMS-specific dump
// config, and returns the set of object IDs that should participate in the dump.
//
// Note: highest-scope filtering (e.g. limiting to specific databases or schemas)
// is the Introspector's responsibility — the Introspector only returns objects
// within the requested scope. ObjectFilter handles finer-grained per-object
// inclusion/exclusion within that already-scoped result.
type ObjectFilter interface {
	FilterObjects(ctx context.Context, input ObjectFilterInput) (ObjectFilterResult, error)
}
