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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/storages"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

type Option func(dump *Dump) error

// Dump is the PostgreSQL dump orchestrator placeholder.
// All methods return "not implemented yet" until the PostgreSQL port is complete.
type Dump struct {
	dumpID               core.DumpID
	cfg                  *config.Config
	st                   core.Storager
	registry             *registry.TransformerRegistry
	cmd                  utils.CmdProducer
	hbw                  *heartbeat.Worker
	hbwEg                *errgroup.Group
	startedAt            time.Time
	dumpStats            core.DataDumpStat
	dumpedDatabaseSchema []core.SchemaDumpStat
	dataOnly             bool
	schemaOnly           bool
}

// New returns a Dump for use as an engines.Dumper.
func New(
	cfg *config.Config,
	reg *registry.TransformerRegistry,
	st core.Storager,
	cmd utils.CmdProducer,
	opts ...Option,
) (*Dump, error) {
	dumpID := core.NewDumpID()
	st = storages.SubStorageWithDumpID(st, dumpID)
	res := &Dump{
		cfg:      cfg,
		st:       st,
		registry: reg,
		cmd:      cmd,
		dumpID:   dumpID,
	}
	for i, opt := range opts {
		if err := opt(res); err != nil {
			return nil, fmt.Errorf("apply dump option %d: %w", i, err)
		}
	}
	return res, nil
}

// NewValidator returns a Dump configured for validation (same placeholder).
func NewValidator(
	cfg *config.Config,
	reg *registry.TransformerRegistry,
	st core.Storager,
	cmd utils.CmdProducer,
) (*Dump, error) {
	return New(cfg, reg, st, cmd)
}

func (d *Dump) Init(_ context.Context) error {
	return errNotImplemented("init")
}

func (d *Dump) Done(_ context.Context) error {
	return errNotImplemented("done")
}

func (d *Dump) StartHBWorker(ctx context.Context) {
	hbInterval := d.cfg.Common.HeartbeatInterval
	if hbInterval <= 0 {
		hbInterval = heartbeat.DefaultWriteInterval
	}
	d.hbw = heartbeat.NewWorker(heartbeat.NewWriter(d.st)).
		SetInterval(hbInterval)
	d.hbwEg, ctx = errgroup.WithContext(ctx)
	d.hbwEg.Go(d.hbw.Run(ctx))
}

func (d *Dump) StopHBWorker(ctx context.Context, err error) error {
	status := heartbeat.StatusDone
	if err != nil {
		status = heartbeat.StatusFailed
	}
	d.hbw.Terminate(status)
	if err := d.hbwEg.Wait(); err != nil {
		log.Ctx(ctx).Warn().Err(err).Msg("failed to wait for heartbeat worker")
	}
	return nil
}

func (d *Dump) Introspect(_ context.Context) error {
	return errNotImplemented("introspect")
}

func (d *Dump) IntrospectAndGetTables(_ context.Context) ([]core.Table, error) {
	return nil, errNotImplemented("introspect-tables")
}

func (d *Dump) SchemaDump(_ context.Context) ([]core.SchemaDumpStat, error) {
	return nil, errNotImplemented("schema-dump")
}

func (d *Dump) DataDump(_ context.Context) error {
	return errNotImplemented("data-dump")
}

func (d *Dump) GetDumpMetadata(_ time.Time) (core.Metadata, error) {
	return core.Metadata{}, errNotImplemented("get-metadata")
}

func (d *Dump) WriteMetadata(_ context.Context) error {
	return errNotImplemented("write-metadata")
}

func (d *Dump) Run(_ context.Context) error {
	return errNotImplemented("run")
}

func (d *Dump) GetDumpID() core.DumpID {
	return d.dumpID
}

func (d *Dump) DumpSample(_ context.Context, _ bool, _ []core.TableFilter) error {
	return errNotImplemented("dump-sample")
}

func (d *Dump) SchemaDiff(_ context.Context) error {
	return errNotImplemented("schema-diff")
}

func (d *Dump) Introspection() []core.Table {
	return nil
}

func (d *Dump) Warnings() []*core.ValidationWarning {
	return nil
}

func errNotImplemented(op string) error {
	return fmt.Errorf("postgresql dump %s: not implemented yet", op)
}
