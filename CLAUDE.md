# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What Is Greenmask

Greenmask is an open-source database anonymization and synthetic data generation tool for PostgreSQL and MySQL. It performs logical database backup dumping, anonymization/transformation via a pipeline of configurable transformers, and restoration. It is stateless (no schema changes required), supports parallel execution, and works with local directory or S3-compatible storage backends.

This repository serves a dual role:
1. **CLI utility** — the `greenmask` binary (built from `cmd/`)
2. **Reusable core library** — imported by `gm-backend` (`github.com/greenmaskio/gm-backend`, located at `../gm-backend` relative to this repo), which wraps the core operations in Temporal workflows to implement full Test Data Management (TDM) capabilities as a service

This dual role is a hard architectural constraint. The `dump`, `validate`, and `restore` execution pipelines **must remain reusable as a library** — callable from code, not just from the CLI. `gm-backend` imports this repo via a `replace` directive in its `go.mod` pointing to `../greenmask`.

## Build Commands

```bash
# Build binary (requires viper_bind_struct tag)
make build
# Equivalent: CGO_ENABLED=0 go build -tags=viper_bind_struct -ldflags="-X main.Version=$(VERSION)" -v -o greenmask ./cmd/

# Install to $GOBIN
make install
```

The `viper_bind_struct` build tag is required — without it, environment variable binding to config structs won't work.

## Test Commands

```bash
# Unit tests (only internal/ and pkg/ directories)
make unittest
# or: go list ./... | grep -E 'internal|pkg' | xargs go test -v

# Run a single test
go test -v ./pkg/common/transformers/... -run TestName

# Coverage report (generates coverage.html)
make coverage

# Integration tests (requires Docker)
make integration
```

## Lint

```bash
make lint
# or: golangci-lint run ./...
```

Enabled linters: `errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`. Config in `.golangci.yml`.

## Architecture Overview

### Core Data Flow

```
Source DB → Schema Dump (pg_dump/mysqldump) → Data Read → Transformation Pipeline → Encoding → Storage
```

On restore: Storage → Decoding → (optional scripts) → Target DB

### Entry Points

`cmd/` contains the Cobra CLI. Each subcommand (`dump`, `restore`, `validate`, `delete`, `list-dumps`, `list-transformers`, `show-transformer`) delegates to `pkg/cmdrun/` for actual execution.

`pkg/cmdrun/` is also the **public library API** for the operations — `gm-backend` calls `cmdrun.RunDumpWithContext()`, `cmdrun.RunMySQLValidate()`, `cmdrun.SetupContext()`, etc. directly. Keep these functions importable and context-driven.

### Key Package Roles

**`pkg/config/`** — YAML/JSON config loading via Viper. Structures: `Config`, `Dump`, `Restore`, `Storage`, `Validate`. Transformer parameters use `mapstructure` for deserialization.

**`pkg/cmdrun/`** — Command execution logic. Receives config, constructs dependencies (engine-specific: PostgreSQL vs MySQL), and runs the operation.

**`pkg/common/interfaces/`** — The core abstractions everything is built on:
- `Transformer` — `Init`, `Done`, `Transform(ctx, Recorder)`, `GetAffectedColumns`, `Describe`
- `Recorder` — Column access (get/set by index or name, null checks, raw bytes)
- `Dumper` / `Restorer` — Top-level operation interfaces
- `TableDriver` — DB-type-specific encoding/decoding
- `Pipeliner` — Transformation pipeline orchestration

**`pkg/common/models/`** — Shared data structures: `Table`, `Column`, `ColumnProperties`, `ColumnValue`, `TransformerConfig`, `DynamicParameterValue`, `DBMSEngine` (PostgreSQL/MySQL enum), `Metadata`.

**`pkg/common/transformers/`** — 60+ built-in transformers (RandomInt, RandomChoice, Email, NoiseDate, Hash, Masking, Cmd, Template, Json, etc.). Each transformer implements the `Transformer` interface. Supports both random and hash (deterministic) engines.

**`pkg/common/pipeline/`** — `TransformationPipeline` orchestrates transformers for a table, handles conditional execution (`when` clauses via `expr-lang/expr`), and dynamic parameter binding.

**`pkg/common/dump/`** — Dump execution context and DB-specific dumpers (postgres, mysql subdirs). `processor/` applies the transformation pipeline to dumped data.

**`pkg/common/restore/`** — Restore logic. `processor/` handles data restoration; `taskmapper/` maps work for parallel execution.

**`pkg/common/subset/`** — Database subsetting (virtual foreign keys, nullable columns, cyclic dependency resolution).

**`pkg/mysql/`** — MySQL-specific implementations: `dbmsdriver/` (type encoders/decoders/scanners), `dump/`, `restore/`, `pool/`, `metadata/`, `models/`.

**`pkg/storages/`** — Storage backends: local directory and S3-compatible (via `aws/aws-sdk-go`).

### Library Reusability Contract

`gm-backend` uses this library by:
1. Translating its own domain config (`spec.ProjectManifest`, `spec.StorageManifest`, `spec.SourceManifest`) into greenmask's `config.Config`
2. Calling `cmdrun.SetupContext()` to inject logging and a `validationcollector.Collector` into the context
3. Calling the appropriate `cmdrun.Run*` function
4. Extracting results via `validationcollector.FromContext(ctx)` and `models.TaskStat`

**Constraints this imposes on all work in this repo:**
- `pkg/cmdrun` functions must accept and return via `context.Context` — do not add CLI-only global state or `os.Exit` calls there
- `pkg/common/interfaces/`, `pkg/common/models/`, `pkg/config/`, `pkg/common/validationcollector/`, and `pkg/storages/` are part of the public API surface — avoid breaking changes to exported types
- Validation warnings/errors must flow through `validationcollector` in context, not to stderr directly, so gm-backend can collect them programmatically
- Storage must be injected (via `Storager` interface), not hardcoded, so gm-backend can supply S3 or validation-only storage

### PostgreSQL vs MySQL Split

The codebase supports two engines. PostgreSQL uses `jackc/pgx` and calls `pg_dump`/`pg_restore`. MySQL uses `go-sql-driver/mysql` and `mysqldump`. Engine-specific code lives in `pkg/mysql/` (MySQL) and within `pkg/common/dump|restore/` (PostgreSQL). `pkg/cmdrun/` wires the right implementation based on `DBMSEngine` from config.

### Adding a New Transformer

1. Create a new file in `pkg/common/transformers/`
2. Implement the `Transformer` interface from `pkg/common/interfaces/`
3. Register it in the transformer registry
4. Define parameter structs using `mapstructure` tags

### Configuration Format

Transformations are defined per-table in the config YAML:

```yaml
dump:
  transformation:
    - schema: "public"
      name: "table_name"
      transformers:
        - name: "RandomEmail"
          params:
            column: "email"
          dynamic_params: {}    # template-based dynamic values
          when: "id > 100"      # optional condition (expr-lang)
      subset_conds: []
      columns_type_override: {}
```

### Key Dependencies

- `spf13/cobra` + `spf13/viper` — CLI and config
- `jackc/pgx` — PostgreSQL driver
- `go-sql-driver/mysql` — MySQL driver  
- `expr-lang/expr` — Condition evaluation in `when` clauses
- `rs/zerolog` — Logging
- `huandu/go-sqlbuilder` — SQL construction for subset queries
- `testcontainers/testcontainers-go` — Integration test containers

## Playground / Local Dev

Docker Compose files in the root start PostgreSQL or MySQL playground environments with pre-filled data, MinIO S3 storage, and Greenmask containers. Config examples are in `playground/`.

```bash
make up   # Start database filler for playground
```
