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

package schema

import (
	"context"
	"fmt"
	"io"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const executable = "mysqldump"

// sectionFilePrefix maps schema sections to their file name prefix.
var sectionFilePrefix = map[commonmodels.DumpSection]string{
	commonmodels.DumpSectionPreData:  "pre",
	commonmodels.DumpSectionPostData: "post",
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

type Dumper struct {
	st               interfaces.Storager
	cmdProducer      utils.CmdProducer
	executable       string
	envs             []string
	mysqlParams      []string // connection/auth params only
	vendorOptions    []string // user-specified vendor options
	genericSettings  commonmodels.DumpScope
	compression      bool
	compressionPgzip bool
}

func New(
	cmd utils.CmdProducer,
	st interfaces.Storager,
	envs []string,
	mysqlParams []string,
	vendorOptions []string,
	genericSettings commonmodels.DumpScope,
	compression bool,
	compressionPgzip bool,
) *Dumper {
	return &Dumper{
		executable:       executable,
		envs:             envs,
		st:               st,
		cmdProducer:      cmd,
		mysqlParams:      mysqlParams,
		vendorOptions:    vendorOptions,
		genericSettings:  genericSettings,
		compression:      compression,
		compressionPgzip: compressionPgzip,
	}
}

func containsOption(opts []string, opt string) bool {
	for _, o := range opts {
		if o == opt {
			return true
		}
	}
	return false
}

// addTableFiltering appends ignore-table flags, the database name, and any
// included table names to args in the order mysqldump expects.
func (d *Dumper) addTableFiltering(args []string, dbname string) []string {
	if excludeTables, ok := d.genericSettings.ExcludeTables[dbname]; ok {
		for _, et := range excludeTables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", dbname, et))
		}
	}
	args = append(args, dbname)
	if tables, ok := d.genericSettings.IncludeTables[dbname]; ok {
		args = append(args, tables...)
	}
	return args
}

// getPreDataCliParameters builds the mysqldump args for the pre-data section:
// table structures without triggers, routines, or events.
func (d *Dumper) getPreDataCliParameters(dbname string) []string {
	args := []string{
		"--no-data",
		"--skip-triggers",
		"--skip-opt",
	}
	args = append(args, d.mysqlParams...)
	for _, opt := range d.vendorOptions {
		if !postDataVendorOptions[opt] {
			args = append(args, opt)
		}
	}
	return d.addTableFiltering(args, dbname)
}

// getPostDataCliParameters builds the mysqldump args for the post-data section:
// triggers, routines, and events without any table or data DDL.
func (d *Dumper) getPostDataCliParameters(dbname string) []string {
	args := []string{
		"--no-create-info",
		"--no-data",
		"--no-create-db",
	}
	args = append(args, d.mysqlParams...)

	// triggers are included by default unless the user explicitly opted out
	if !containsOption(d.vendorOptions, "--skip-triggers") {
		args = append(args, "--triggers")
	}
	if containsOption(d.vendorOptions, "--routines") {
		args = append(args, "--routines")
	}
	if containsOption(d.vendorOptions, "--events") {
		args = append(args, "--events")
	}
	if containsOption(d.vendorOptions, "--add-drop-trigger") {
		args = append(args, "--add-drop-trigger")
	}

	for _, opt := range d.vendorOptions {
		if !postDataVendorOptions[opt] {
			args = append(args, opt)
		}
	}
	return d.addTableFiltering(args, dbname)
}

// DumpPreDataSchema dumps the pre-data section (tables, views — no triggers/routines/events)
// for every allowed schema.
func (d *Dumper) DumpPreDataSchema(ctx context.Context) ([]commonmodels.SchemaDumpStat, error) {
	res := make([]commonmodels.SchemaDumpStat, 0, len(d.genericSettings.AllowedSchemas))
	for _, dbname := range d.genericSettings.AllowedSchemas {
		stat, err := d.dumpDatabaseSection(ctx, dbname, commonmodels.DumpSectionPreData)
		if err != nil {
			return nil, fmt.Errorf("database '%s' pre-data: %w", dbname, err)
		}
		res = append(res, stat)
	}
	return res, nil
}

// DumpPostDataSchema dumps the post-data section (triggers, routines, events)
// for every allowed schema.
func (d *Dumper) DumpPostDataSchema(ctx context.Context) ([]commonmodels.SchemaDumpStat, error) {
	res := make([]commonmodels.SchemaDumpStat, 0, len(d.genericSettings.AllowedSchemas))
	for _, dbname := range d.genericSettings.AllowedSchemas {
		stat, err := d.dumpDatabaseSection(ctx, dbname, commonmodels.DumpSectionPostData)
		if err != nil {
			return nil, fmt.Errorf("database '%s' post-data: %w", dbname, err)
		}
		res = append(res, stat)
	}
	return res, nil
}

func (d *Dumper) dumpDatabaseSection(
	ctx context.Context,
	dbname string,
	section commonmodels.DumpSection,
) (commonmodels.SchemaDumpStat, error) {
	// Resolve CLI params before creating the pipe. An unknown-section error
	// here would otherwise leave w and r unclosed (pipe-end leak).
	var params []string
	switch section {
	case commonmodels.DumpSectionPreData:
		params = d.getPreDataCliParameters(dbname)
	case commonmodels.DumpSectionPostData:
		params = d.getPostDataCliParameters(dbname)
	default:
		return commonmodels.SchemaDumpStat{}, fmt.Errorf("unknown schema section: %s", section)
	}

	var r utils.CountReadCloser
	var w utils.CountWriteCloser
	if d.compression {
		w, r = utils.NewGzipPipe(d.compressionPgzip)
	} else {
		w, r = utils.NewPlainPipe()
	}

	fileName := fmt.Sprintf("schema_%s_%s.sql", sectionFilePrefix[section], dbname)
	if d.compression {
		fileName += ".gz"
	}

	ctx = log.Ctx(ctx).With().
		Str("Stage", "SchemaDump").
		Str("Section", string(section)).
		Str("Database", dbname).
		Str("FileName", fileName).
		Logger().WithContext(ctx)

	eg, gtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer func() {
			if err := r.Close(); err != nil {
				log.Ctx(ctx).Warn().Err(err).Msg("error closing input reader")
			}
		}()
		if err := d.st.PutObject(gtx, fileName, r); err != nil {
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
		cmd, err := d.cmdProducer.Produce(d.executable, params, d.envs, nil)
		if err != nil {
			return fmt.Errorf("cannot produce mysqldump command: %w", err)
		}
		// Use gtx so the subprocess is cancelled if the reader goroutine fails.
		if err := cmd.ExecuteCmdAndWriteStdout(gtx, w); err != nil {
			return fmt.Errorf("cannot run mysqldump for database %s section %s: %w", dbname, section, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return commonmodels.SchemaDumpStat{}, err
	}

	compression := commonmodels.CompressionNone
	if d.compression {
		compression = commonmodels.CompressionGzip
		if d.compressionPgzip {
			compression = commonmodels.CompressionPgzip
		}
	}

	return commonmodels.SchemaDumpStat{
		DatabaseName:   dbname,
		FileName:       fileName,
		Section:        section,
		Compression:    compression,
		OriginalSize:   w.GetCount(),
		CompressedSize: r.GetCount(),
	}, nil
}
