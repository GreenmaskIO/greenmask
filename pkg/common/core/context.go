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
}
