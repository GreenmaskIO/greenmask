// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package engines defines the top-level orchestrator interfaces and the factory
// functions that instantiate the correct engine implementation based on config.
//
// There is exactly one engine switch in this package (in factory.go). No other
// package should switch on cfg.Engine to select a DBMS implementation.
package engines

import (
	"context"
	"time"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

// Dumper is the engine-level dump orchestrator.
// It mirrors the exported methods of pkg/mysql/cli/dump.Dump so that
// callers (pkg/cli, gm-backend) can drive individual lifecycle steps or
// simply call Run for the full flow.
//
// Note: pkg/common/interfaces also has a Dumper, but that is a task-level
// interface (one table). This is the operation-level orchestrator.
type Dumper interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	StartHBWorker(ctx context.Context)
	StopHBWorker(ctx context.Context, err error) error
	Introspect(ctx context.Context) error
	IntrospectAndGetTables(ctx context.Context) ([]models.Table, error)
	SchemaDump(ctx context.Context) ([]models.SchemaDumpStat, error)
	DataDump(ctx context.Context) error
	GetDumpMetadata(completedAt time.Time) (models.Metadata, error)
	WriteMetadata(ctx context.Context) error
	Run(ctx context.Context) error
	GetDumpID() models.DumpID
}

// Restorer is the engine-level restore orchestrator.
// It mirrors the exported methods of pkg/mysql/cli/restore.Restore.
type Restorer interface {
	Run(ctx context.Context) error
}

// Validator is the engine-level validate orchestrator.
//
// Unlike Dumper, Validator exposes a focused set of methods oriented around
// the validate workflow: initialising a connection, driving a data sample
// (optionally with before/after diff), inspecting schema differences, and
// retrieving collected warnings. The validate result is also available via
// validationcollector in the context after Run returns.
type Validator interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	Run(ctx context.Context) error
	// DumpSample executes a data dump for the given tables and stores the
	// result in the validate storage so that it can be printed afterward.
	// When diff is true, original (pre-transformation) values are captured
	// alongside the transformed ones.
	DumpSample(ctx context.Context, diff bool, tables []models.TableFilter) error
	// SchemaDiff checks for mismatches between the transformer configuration
	// and the current DB schema, collecting any discrepancies as warnings.
	SchemaDiff(ctx context.Context) error
	// Introspection returns the tables discovered during the most recent Run
	// or Introspect call. The CMD layer uses this to compare the current DB
	// schema against a previous dump's stored schema.
	Introspection() []models.Table
	// Warnings returns all validation warnings collected during the run.
	Warnings() []*models.ValidationWarning
}
