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

package dump

import (
	"context"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/config"
)

// Dump is the PostgreSQL dump orchestrator placeholder.
// All methods return "not implemented yet" until the PostgreSQL port is complete.
type Dump struct {
	cfg *config.Config
	st  interfaces.Storager
}

// New returns a Dump for use as an engines.Dumper.
func New(cfg *config.Config, st interfaces.Storager) (*Dump, error) {
	return &Dump{cfg: cfg, st: st}, nil
}

// NewValidator returns a Dump configured for validation (same placeholder).
func NewValidator(cfg *config.Config, st interfaces.Storager) (*Dump, error) {
	return New(cfg, st)
}

func (d *Dump) Init(_ context.Context) error {
	return errNotImplemented("init")
}

func (d *Dump) Done(_ context.Context) error {
	return errNotImplemented("done")
}

func (d *Dump) StartHBWorker(_ context.Context) {}

func (d *Dump) StopHBWorker(_ context.Context, _ error) error {
	return nil
}

func (d *Dump) Introspect(_ context.Context) error {
	return errNotImplemented("introspect")
}

func (d *Dump) IntrospectAndGetTables(_ context.Context) ([]models.Table, error) {
	return nil, errNotImplemented("introspect-tables")
}

func (d *Dump) SchemaDump(_ context.Context) ([]models.DumpedDatabaseSchemaStat, error) {
	return nil, errNotImplemented("schema-dump")
}

func (d *Dump) DataDump(_ context.Context) error {
	return errNotImplemented("data-dump")
}

func (d *Dump) GetDumpMetadata(_ time.Time) (models.Metadata, error) {
	return models.Metadata{}, errNotImplemented("get-metadata")
}

func (d *Dump) WriteMetadata(_ context.Context) error {
	return errNotImplemented("write-metadata")
}

func (d *Dump) Run(_ context.Context) error {
	return errNotImplemented("run")
}

func (d *Dump) GetDumpID() models.DumpID {
	return ""
}

func (d *Dump) DumpSample(_ context.Context, _ bool, _ []models.TableFilter) error {
	return errNotImplemented("dump-sample")
}

func (d *Dump) SchemaDiff(_ context.Context) error {
	return errNotImplemented("schema-diff")
}

func (d *Dump) Introspection() []models.Table {
	return nil
}

func (d *Dump) Warnings() []*models.ValidationWarning {
	return nil
}

func errNotImplemented(op string) error {
	return fmt.Errorf("postgresql dump %s: not implemented yet", op)
}
