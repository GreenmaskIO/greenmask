package core

type DumpContextDiffInput struct {
	Previous *DumpContextSnapshot
	Current  DumpContextSnapshot
}

// DumpContextDiff is the result of comparing two DumpContextSnapshot instances.
//
// The structure is intentionally open-ended — concrete diff semantics will be
// defined when the differ implementation is built. Before implementing it, decide
// on the result shape; the requirements below capture what it must satisfy.
//
// Main requirement: report the full added / removed / modified set, like a text
// diff's + / - / ~, at every level of the snapshot tree — not just a boolean
// "something changed". Consumers (DumpContextValidator, DumpPlanAssembler, and
// gm-backend approval/GitOps/UI/audit workflows) branch on this, and it is
// serialized into RunState/Metadata across the Temporal boundary, so the result
// must be structured (machine-consumable), deterministic, and JSON-serializable.
//
// Things to consider when implementing the differ:
//
//   - Recursion depth: every collection in the snapshot should report its own
//     added/removed/changed set — Objects, each object's Attributes (columns) and
//     Transformations, and the transformer Config/StaticParameters/DynamicParameters
//     and source Filters/VendorParameters bags. A single top-level "changed" flag
//     is not enough.
//
//   - Hashes as short-circuits: the snapshot already carries Fingerprint,
//     AttributesHash, ConfigHash, SubsetQueryHash, VendorParametersHash, etc. Use
//     them to skip unchanged subtrees cheaply; only descend to per-element detail
//     when a hash differs. The semantic-normalization work already lives in those
//     hashes.
//
//   - Classification: tag changes by significance so consumers can gate on them —
//     e.g. data-affecting (transformations, subset, conditions, NeedDumpData) vs.
//     schema-shape (objects/attributes, NeedSchemaDump) vs. informational (DBMS
//     version, vendor params, identity, provenance). gm-backend should be able to
//     block only on data-affecting drift without re-deriving meaning per field.
//
//   - Determinism: walk maps in sorted key order so identical inputs always yield
//     byte-identical output (required for stable serialization and diffing in tests).
//
//   - Payload size: do not embed the full Previous/Current snapshots in the JSON —
//     they are already persisted in Metadata. Prefer which-part + hash deltas over
//     full before/after value maps.
//
//   - Initial run (Previous == nil) and SchemaVersionChanged are special cases: the
//     first must mark everything added; the second must suppress false-positive
//     drift on fields absent from the older schema version.
//
//   - Engine-agnostic: the algorithm operates purely on DumpContextSnapshot, which
//     is identical across PostgreSQL/MySQL, so the implementation should be shared
//     in this package and wired by every engine rather than reimplemented per engine.
type DumpContextDiff struct {
	Previous *DumpContextSnapshot
	Current  DumpContextSnapshot
	// SchemaVersionChanged is true when Previous and Current were produced by
	// different snapshot schema versions. The differ should suppress false-positive
	// drift on fields that did not exist in the older schema version.
	SchemaVersionChanged bool
}
