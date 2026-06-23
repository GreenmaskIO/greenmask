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

// Package schema implements the MySQL schema (DDL) dump factory. Each dumper it
// builds owns the mysqldump invocation for a single database+section. The
// runtime resources — destination storage and connection attributes — are
// injected into the dumper's Dump call; at Dump time it transforms the
// connection attributes into mysqldump CLI parameters and streams the resulting
// DDL directly into the supplied storage.
package schema

import (
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

var (
	_ core.SchemaDumpFactory = (*Factory)(nil)
	_ core.SchemaDumper      = (*dumper)(nil)
)

// sectionFilePrefix maps schema sections to their file name prefix.
var sectionFilePrefix = map[core.DumpSection]string{
	core.DumpSectionPreData:  "pre",
	core.DumpSectionPostData: "post",
}

// postDataVendorOptions are flags that control triggers/routines/events.
// They are handled explicitly when building post-data parameters, so they
// must be excluded from the generic vendor options pass-through.
var postDataVendorOptions = map[string]bool{
	"--triggers":         true,
	"--skip-triggers":    true,
	"--routines":         true,
	"--events":           true,
	"--add-drop-trigger": true,
}

// Payload is the MySQL-specific schema dump context carried by SchemaDumpSpec.
// It is produced by the dump context builder from configuration and consumed by
// this factory to construct the mysqldump-backed dumper. The connection
// attributes (environment, connection flags, vendor options) are not carried
// here: they are injected at Dump time via the ConnectionConfigurer.
type Payload struct {
	// Name is the database whose schema is dumped.
	Name string
	// Section is the schema section to dump (pre-data or post-data).
	Section core.DumpSection
	// Scope carries table-level include/exclude filtering for the schema DDL.
	// Not wired from the dump context builder yet; an empty scope dumps every
	// table's structure in the database.
	Scope core.DumpScope
	// Compression controls gzip output (CompressionPgzip selects the parallel implementation).
	Compression core.Compression
}

// connAttributes is the subset of the MySQL connection configurer the schema
// dumper needs to assemble a mysqldump invocation. The MySQL ConnectionConfigurer's
// ConnectionConfig() value (*dump.DumpConnectionConfig) satisfies it; asserting
// against this interface lets the factory avoid importing that concrete type,
// which would form an import cycle (dump -> factory -> factory/schema -> dump).
type connAttributes interface {
	MysqldumpEnv() ([]string, error)
	MysqldumpConnParams() []string
	MysqldumpVendorOptions() []string
}

// Factory builds MySQL schema dumpers. Runtime resources (storage, connection
// attributes) are injected into each dumper's Dump call, so the factory is
// storage- and connection-free. The vendor-utility provider (mysqldump) is
// injected so the dumper neither builds nor executes the command directly.
type Factory struct {
	provider core.VendorUtilityProvider
}

// NewFactory creates the MySQL schema dump factory backed by the supplied
// mysqldump vendor-utility provider.
func NewFactory(provider core.VendorUtilityProvider) *Factory {
	return &Factory{provider: provider}
}

func (f *Factory) Kind() core.SchemaObjectKind {
	return core.SchemaObjectKindMysqlDatabase
}

func (f *Factory) New(spec core.SchemaDumpSpec) (core.SchemaDumper, error) {
	payload, ok := spec.Payload.(Payload)
	if !ok {
		return nil, fmt.Errorf("expected schema.Payload, got %T", spec.Payload)
	}
	return &dumper{
		provider:    f.provider,
		database:    payload.Name,
		section:     payload.Section,
		scope:       payload.Scope,
		compression: payload.Compression,
	}, nil
}

// dumper is the executable MySQL schema dumper for a single database+section.
// It performs the mysqldump invocation itself, deriving the CLI environment and
// parameters from the connection attributes handed to Dump and streaming the
// resulting DDL into the supplied storage.
type dumper struct {
	provider    core.VendorUtilityProvider
	database    string
	section     core.DumpSection
	scope       core.DumpScope
	compression core.Compression
}

func (d *dumper) Dump(ctx context.Context, conn core.ConnectionConfigurer, st core.Storager) (core.SchemaDumpStat, error) {
	attrs, ok := conn.ConnectionConfig().(connAttributes)
	if !ok {
		return core.SchemaDumpStat{}, fmt.Errorf(
			"connection config %T does not provide mysqldump attributes", conn.ConnectionConfig(),
		)
	}
	envs, err := attrs.MysqldumpEnv()
	if err != nil {
		return core.SchemaDumpStat{}, fmt.Errorf("build mysqldump environment: %w", err)
	}
	connParams := attrs.MysqldumpConnParams()
	vendorOptions := attrs.MysqldumpVendorOptions()

	// Resolve CLI params before creating the pipe. An unknown-section error
	// here would otherwise leave w and r unclosed (pipe-end leak).
	var params []string
	switch d.section {
	case core.DumpSectionPreData:
		params = d.getPreDataCliParameters(connParams, vendorOptions)
	case core.DumpSectionPostData:
		params = d.getPostDataCliParameters(connParams, vendorOptions)
	default:
		return core.SchemaDumpStat{}, fmt.Errorf("unknown schema section: %s", d.section)
	}

	// An unset compression leaves the zero value (""); treat it as no
	// compression so IsEnabled()/the pipe switch behave consistently.
	compression := d.compression
	if compression == "" {
		compression = core.CompressionNone
	}

	var r utils.CountReadCloser
	var w utils.CountWriteCloser
	switch compression {
	case core.CompressionGzip:
		w, r = utils.NewGzipPipe(false)
	case core.CompressionPgzip:
		w, r = utils.NewGzipPipe(true)
	default: // CompressionNone
		w, r = utils.NewPlainPipe()
	}

	fileName := fmt.Sprintf("schema_%s_%s.sql", sectionFilePrefix[d.section], d.database) + compression.GetExt()

	ctx = log.Ctx(ctx).With().
		Str("Stage", "SchemaDump").
		Str("Section", string(d.section)).
		Str("Database", d.database).
		Str("FileName", fileName).
		Logger().WithContext(ctx)

	eg, gtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer func() {
			if err := r.Close(); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("error closing input reader")
			}
		}()
		if err := st.PutObject(gtx, fileName, r); err != nil {
			return fmt.Errorf("put schema %s: %w", fileName, err)
		}
		return nil
	})

	eg.Go(func() error {
		defer func(w io.Closer) {
			if err := w.Close(); err != nil {
				log.Ctx(ctx).Error().Err(err).Msg("error closing output writer")
			}
		}(w)
		// Stream mysqldump stdout into the storage writer. Use gtx so the
		// subprocess is cancelled if the reader goroutine fails.
		if err := d.provider.Stream(gtx, params, envs, nil, w); err != nil {
			return fmt.Errorf("cannot run %s for database %s section %s: %w", d.provider.Name(), d.database, d.section, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return core.SchemaDumpStat{}, err
	}

	// Record which vendor utility produced this schema dump. A version-probe
	// failure must not fail the dump itself — the DDL is already persisted — so
	// it is logged and the utility identity is left unset.
	var vendorUtility *core.VendorUtility
	if vu, err := d.provider.Version(ctx); err != nil {
		log.Ctx(ctx).Warn().Err(err).Str("Executable", d.provider.Name()).Msg("probe vendor utility version")
	} else {
		vendorUtility = &vu
	}

	return core.SchemaDumpStat{
		Kind:           core.SchemaObjectKindMysqlDatabase,
		DatabaseName:   d.database,
		FileName:       fileName,
		Section:        d.section,
		Compression:    compression,
		OriginalSize:   w.GetCount(),
		CompressedSize: r.GetCount(),
		VendorUtility:  vendorUtility,
	}, nil
}

// getPreDataCliParameters builds the mysqldump args for the pre-data section:
// table structures without triggers, routines, or events.
func (d *dumper) getPreDataCliParameters(connParams, vendorOptions []string) []string {
	args := []string{
		"--no-data",
		"--skip-triggers",
		"--skip-opt",
	}
	args = append(args, connParams...)
	for _, opt := range vendorOptions {
		if !postDataVendorOptions[opt] {
			args = append(args, opt)
		}
	}
	return d.addTableFiltering(args)
}

// getPostDataCliParameters builds the mysqldump args for the post-data section:
// triggers, routines, and events without any table or data DDL.
func (d *dumper) getPostDataCliParameters(connParams, vendorOptions []string) []string {
	args := []string{
		"--no-create-info",
		"--no-data",
		"--no-create-db",
	}
	args = append(args, connParams...)

	// triggers are included by default unless the user explicitly opted out
	if !slices.Contains(vendorOptions, "--skip-triggers") {
		args = append(args, "--triggers")
	}
	if slices.Contains(vendorOptions, "--routines") {
		args = append(args, "--routines")
	}
	if slices.Contains(vendorOptions, "--events") {
		args = append(args, "--events")
	}
	if slices.Contains(vendorOptions, "--add-drop-trigger") {
		args = append(args, "--add-drop-trigger")
	}

	for _, opt := range vendorOptions {
		if !postDataVendorOptions[opt] {
			args = append(args, opt)
		}
	}
	return d.addTableFiltering(args)
}

// addTableFiltering appends ignore-table flags, the database name, and any
// included table names to args in the order mysqldump expects.
func (d *dumper) addTableFiltering(args []string) []string {
	if excludeTables, ok := d.scope.ExcludeTables[d.database]; ok {
		for _, et := range excludeTables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", d.database, et))
		}
	}
	args = append(args, d.database)
	if tables, ok := d.scope.IncludeTables[d.database]; ok {
		args = append(args, tables...)
	}
	return args
}

func (d *dumper) DebugInfo() string {
	return fmt.Sprintf("mysql schema dump: database=%s section=%s", d.database, d.section)
}

func (d *dumper) Meta() map[string]any {
	return map[string]any{
		"database": d.database,
		"section":  d.section,
	}
}
