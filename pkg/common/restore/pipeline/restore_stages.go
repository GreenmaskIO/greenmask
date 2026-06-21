package pipeline

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// RestoreStages is the pluggable set of stage interfaces that constitute a
// restore pipeline. Each field is implemented per DBMS; the pipeline itself
// imports no DBMS-specific packages.
//
// The pipeline has three logical phases:
//
//   - Runtime: open a session to the target DB and provision storage scoped
//     to the requested dumpID.
//   - Metadata: read the dump metadata from the provisioned storage.
//   - Execution: build the restore instruction and run the restore processor.
//
// This is intentionally simpler than DumpStages — there are no discovery,
// context-building, snapshot, diff, or plan-assembly stages because the dump
// plan is already encoded in metadata.json.
type RestoreStages struct {
	// ConnectionConfigurerBuilder translates the generic config.Config (restore
	// section) into a DBMS-specific ConnectionConfigurer carrying target-DB
	// connection parameters.
	//
	// This is the same interface as used by DumpStages, but implementations
	// read from cfg.Restore.* rather than cfg.Dump.*.
	ConnectionConfigurerBuilder core.ConnectionConfigurerBuilder

	// DatabaseSessionBuilder opens a DBMS-specific connection to the target DB.
	DatabaseSessionBuilder core.DatabaseSessionBuilder

	// RestoreStorageProvisioner provisions a Storager scoped to the resolved
	// dumpID. It handles "latest" resolution internally.
	RestoreStorageProvisioner core.RestoreStorageProvisioner

	// RestoreMetadataReader reads metadata.json from the provisioned storage.
	RestoreMetadataReader core.RestoreMetadataReader

	// RestoreInstructionBuilder extracts execution-time parameters from config
	// (jobs, data-only, schema-only, sections, scripts, vendor options, etc.).
	RestoreInstructionBuilder core.RestoreInstructionBuilder

	// RestorePlanBuilder converts Metadata into a RestorePlan with typed Payload
	// fields. It is the deserialization boundary between persisted metadata JSON
	// and runtime Go structs — each engine registers its own implementation.
	RestorePlanBuilder core.RestorePlanBuilder

	// RestoreProcessor executes the restore operation: pre-data schema, data,
	// and post-data schema phases.
	RestoreProcessor core.RestoreProcessor
}
