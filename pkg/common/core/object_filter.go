package core

import "context"

// FilterConfig carries the DBMS-agnostic include/exclude filtering options
// extracted from the user config by a FilterConfigBuilder. It formalizes the
// higher-level object-selection options (databases, schemas, table patterns)
// that ObjectFilter uses to decide which introspected objects participate in
// the dump.
type FilterConfig struct {
	IncludeDatabase        []string
	ExcludeDatabase        []string
	IncludeSchema          []string
	ExcludeSchema          []string
	IncludeTableData       []string
	ExcludeTableData       []string
	IncludeTable           []string
	ExcludeTable           []string
	IncludeTableDefinition []string
	ExcludeTableDefinition []string
	DataOnly               bool
	SchemaOnly             bool
}

// FilterConfigBuilder extracts the DBMS-agnostic include/exclude filter options
// from the full config into a FilterConfig consumed by ObjectFilter.
//
// Build receives the full config as any to avoid an import cycle
// (pkg/config already imports this package). Implementations type-assert to
// config.Config internally.
type FilterConfigBuilder interface {
	Build(cfg any) (FilterConfig, error)
}

// ObjectFilterInput is passed to ObjectFilter.FilterObjects.
type ObjectFilterInput struct {
	IntrospectionResult IntrospectionResult
	// FilterConfig carries the DBMS-agnostic include/exclude filtering options
	// produced by FilterConfigBuilder.
	FilterConfig FilterConfig
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
