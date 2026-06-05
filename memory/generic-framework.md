# Generic Dump/Restore Framework (v1 Abstraction Layer)

This document describes the engine-agnostic abstraction layer introduced in the v1 refactor. It lives in `pkg/common/interfaces/`, `pkg/common/models/`, `pkg/common/dump/pipeline/`, and `pkg/common/dumpfactory/`.

---

## Goal

Decouple the dump/restore/validate orchestration from any specific DBMS so that PostgreSQL and MySQL (and future engines) share a single pipeline implementation. Engine-specific code is confined to implementations that satisfy the interfaces below.

---

## Core Interfaces (`pkg/common/interfaces/`)

### Session & Connection

| Interface | File | Purpose |
|---|---|---|
| `ConnectionConfigurer` | `connection_configurer.go` | Carries DBMS-specific connection parameters. Produced by `ConnectionConfigurerBuilder` from `config.Config`. |
| `ConnectionConfigurerBuilder` | `connection_configurer.go` | Translates `config.Config` → `ConnectionConfigurer`. Only place the pipeline touches DBMS config layout. |
| `DumpSession` | `dump_session.go` | Long-lived runtime session for the full dump execution: pools, transactions, snapshots. Provides `OperationalDB()` (generic SQL) and `EngineResources()` (DBMS-specific, typed as `any`). |
| `DumpSessionBuilder` | `dump_session.go` | Opens a `DumpSession` from a `ConnectionConfigurer`. |
| `DB` | `db.go` | Minimal common SQL interface used by generic stages (introspection, validation, metadata). |

### Introspection & Planning

| Interface | File | Purpose |
|---|---|---|
| `IntrospectorV2` | `introspector.go` | Reads live DB schema into a `SourceSnapshot`. |
| `DependencyGraphBuilder` | `dependency_graph_builder.go` | Builds object dependency graph from the snapshot. |
| `SubsetBuilder` | `subset_builder.go` | Adds virtual FK / subset conditions to the plan. |
| `SchemaDriftValidator` | `schema_drift_validator.go` | Detects schema drift between current DB and a previous `DumpContext`. |
| `DumpContextSnapshotBuilder` | `dump_context_snapshot_builder.go` | Builds a `DumpContextSnapshot` from a `SourceSnapshot`. |
| `DerivedDumpContextBuilder` | `dump_context_builder.go` | Derives a new `DumpContext` from config + snapshot. |
| `ExplicitDumpContextBuilder` | `dump_context_builder.go` | Builds a `DumpContext` directly from an explicit input (no live DB needed). |
| `DumpContextDiffer` | `dump_context_differ.go` | Diffs two `DumpContext` values into a `DumpContextDiff`. |
| `DumpContextValidator` | `dump_context_snapshot_builder.go` | Validates a `DumpContext` and produces warnings/errors. |
| `DumpPlanAssembler` | `dump_plan_assembler.go` | Assembles the final `DumpPlan` from context + graph. |
| `DumpPlanValidator` | `dump_pipeline_validator.go` | Validates the assembled `DumpPlan`. |
| `RestorationContextBuilder` | — | Builds `RestorationContext` for the restore phase. |

### Execution

| Interface | File | Purpose |
|---|---|---|
| `DumpObjectProducerV2` | `task_producer.go` | Produces object dump tasks (tables, sequences, blobs, …) from the plan. |
| `ObjectDumper` | `dumper.go` | Dumps a single object (e.g. one table). |
| `SchemaDumper` | `dumper.go` | Dumps schema DDL for a single schema object. |
| `ObjectDumpFactory` | `factory.go` | Creates `ObjectDumper` instances by `ObjectKind`. Type alias of `DumpFactory[ObjectKind, ObjectDumpSpec, ObjectDumper]`. |
| `SchemaDumpFactory` | `factory.go` | Creates `SchemaDumper` instances by `SchemaDumpKind`. |
| `ObjectDumpFactoryRegistry` | `factory.go` | Registry mapping `ObjectKind` → `ObjectDumpFactory`. |
| `SchemaDumpFactoryRegistry` | `factory.go` | Registry mapping `SchemaDumpKind` → `SchemaDumpFactory`. |
| `DumpProcessor` | `dump_processor.go` | Runs the data transformation pipeline over a dumped row stream. |
| `DumpMetadataLoader` | `meta_loader.go` | Loads dump metadata from storage. |
| `ConfigEditor` | `config_editor.go` | Applies runtime config edits (e.g. dynamic parameter injection). |

### Shared / Pre-existing

| Interface | File | Purpose |
|---|---|---|
| `Transformer` | `transformer.go` | Row-level transformation step. |
| `Recorder` | `recorder.go` | Column access during transformation (get/set by index or name). |
| `Pipeliner` | `pipeliner.go` | Orchestrates a sequence of `Transformer` steps for a table. |
| `Storager` | `storager.go` | Storage backend (local dir or S3). Injected; never hardcoded. |
| `TableDriver` / `RowDriver` | `driver.go` / `row_driver.go` | DBMS-specific row encoding/decoding. |
| `TaskMapper` | `task_mapper.go` | Maps restore work for parallel execution. |
| `RestoreTaskProducer` | `restore_task_producer.go` | Produces restore tasks from stored dump artifacts. |

---

## Key Models (`pkg/common/models/`)

| Model | Purpose |
|---|---|
| `SourceSnapshot` | Full introspection result: tables, columns, constraints, indexes, sequences, FKs, etc. |
| `DumpContext` | Enriched, config-merged view of the database ready for dump planning. |
| `DumpContextDiff` | Delta between two `DumpContext` values (schema drift report). |
| `DumpContextSnapshot` | Serialisable snapshot of a `DumpContext` stored alongside the dump artifact. |
| `DumpPlan` | Immutable executable dump plan: ordered list of tasks with specs. |
| `DumpPlanInput` | Input to `DumpPlanAssembler`. |
| `DumpScope` | Scope filters: included/excluded databases, tables, sections. |
| `ObjectDumpSpec` | Spec for dumping one object (kind + parameters). |
| `SchemaDumpSpec` | Spec for dumping one schema object. |
| `ObjectKind` / `SchemaDumpKind` | Discriminants used by factory registries. |
| `DumpProcessorConfig` | Config for the data transformation processor. |
| `RestorationContext` | Context passed to the restore pipeline. |
| `DBMSVersion` | Engine + version info collected during introspection. |
| `Object` / `ObjectNode` / `ObjectGraph` | Generic dependency graph primitives. |

---

## Pipeline Orchestration (`pkg/common/dump/pipeline/`)

`DumpPipeline` owns the end-to-end dump lifecycle. It is constructed with a `DumpStages` struct that wires all the interfaces above:

```
OpenRuntime()   → ConnectionConfigurerBuilder → DumpSessionBuilder → Runtime{Session}
Discover()      → IntrospectorV2 → DependencyGraphBuilder → SubsetBuilder → SchemaDriftValidator
Plan()          → DumpContextSnapshotBuilder → DerivedDumpContextBuilder → DumpContextDiffer
                  → DumpContextValidator → DumpPlanAssembler → DumpPlanValidator
Execute()       → DumpObjectProducerV2 → ObjectDumpFactoryRegistry / SchemaDumpFactoryRegistry
                  → DumpProcessor (transformation pipeline)
```

The pipeline never imports any DBMS package. All DBMS-specific behaviour enters through `DumpStages`.

---

## Factory Registry (`pkg/common/dumpfactory/`)

`registry.go` provides a concrete `FactoryRegistry` implementation. Each DBMS registers its own factories at startup (e.g. `table`, `sequence`, `blob` object kinds). The pipeline calls `registry.New(kind, spec)` and gets back the right `ObjectDumper` or `SchemaDumper`.

---

## Engine Implementations

| Engine | Package | Status |
|---|---|---|
| MySQL | `pkg/mysql/` | Complete — dump, restore, integration tests |
| PostgreSQL | `pkg/postgresql/` + `pkg/postgresql2/` | In progress — dump scaffolded, restore/validate pending |

See [`tasks/postgresql-port.md`](../tasks/postgresql-port.md) for the PostgreSQL port status.
