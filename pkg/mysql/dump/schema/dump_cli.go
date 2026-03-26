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

type Dumper struct {
	st               interfaces.Storager
	cmdProducer      utils.CmdProducer
	executable       string
	envs             []string
	mysqlParams      []string
	genericSettings  commonmodels.MysqlDumpRelatedSettings
	compression      bool
	compressionPgzip bool
}

func New(
	cmd utils.CmdProducer,
	st interfaces.Storager,
	envs []string,
	mysqlParams []string,
	genericSettings commonmodels.MysqlDumpRelatedSettings,
	compression bool,
	compressionPgzip bool,
) *Dumper {
	return &Dumper{
		executable:       executable,
		envs:             envs,
		st:               st,
		cmdProducer:      cmd,
		mysqlParams:      mysqlParams,
		genericSettings:  genericSettings,
		compression:      compression,
		compressionPgzip: compressionPgzip,
	}
}

func (d *Dumper) getCliParameter(dbname string) []string {
	args := []string{"--no-data"} // Dump only schema, no data

	args = append(args, d.mysqlParams...)

	// Add ignored tables for this database. Note that mysqldump requires database.table format for --ignore-table
	// These are named parameters (options) and should come before the positional arguments.
	if excludeTables, ok := d.genericSettings.ExcludeTables[dbname]; ok {
		for _, et := range excludeTables {
			args = append(args, fmt.Sprintf("--ignore-table=%s.%s", dbname, et))
		}
	}

	// Add database name as the first positional argument
	args = append(args, dbname)

	// Add included tables for this database as subsequent positional arguments
	if tables, ok := d.genericSettings.IncludeTables[dbname]; ok {
		args = append(args, tables...)
	}

	return args
}

func (d *Dumper) DumpSchema(ctx context.Context) ([]commonmodels.DumpedDatabaseSchemaStat, error) {
	res := make([]commonmodels.DumpedDatabaseSchemaStat, 0)
	for _, dbname := range d.genericSettings.AllowedSchemas {
		stat, err := d.dumpDatabaseSchema(ctx, dbname)
		if err != nil {
			return nil, fmt.Errorf("database '%s': %w", dbname, err)
		}
		res = append(res, stat)
	}
	return res, nil
}

func (d *Dumper) dumpDatabaseSchema(ctx context.Context, dbname string) (commonmodels.DumpedDatabaseSchemaStat, error) {
	var r utils.CountReadCloser
	var w utils.CountWriteCloser
	if d.compression {
		w, r = utils.NewGzipPipe(d.compressionPgzip)
	} else {
		w, r = utils.NewPlainPipe()
	}

	eg, gtx := errgroup.WithContext(ctx)
	fileName := fmt.Sprintf("schema_%s.sql", dbname)
	if d.compression {
		fileName += ".gz"
	}
	ctx = log.Ctx(ctx).With().
		Str("Stage", "SchemaDump").
		Str("Database", dbname).
		Str("FileName", fileName).
		Logger().WithContext(ctx)

	eg.Go(func() error {
		defer func() {
			if err := r.Close(); err != nil {
				log.Ctx(ctx).Warn().
					Err(err).
					Msg("error closing input reader")
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
				log.Ctx(ctx).Error().
					Err(err).
					Msg("error closing output writer")
			}
		}(w)
		params := d.getCliParameter(dbname)
		cmd, err := d.cmdProducer.Produce(d.executable, params, d.envs, nil)
		if err != nil {
			return fmt.Errorf("cannot produce mysqldump command: %w", err)
		}
		if err := cmd.ExecuteCmdAndWriteStdout(ctx, w); err != nil {
			return fmt.Errorf("cannot run mysqldump for database %s: %w", dbname, err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return commonmodels.DumpedDatabaseSchemaStat{}, err
	}

	compression := commonmodels.CompressionNone
	if d.compression {
		compression = commonmodels.CompressionGzip
		if d.compressionPgzip {
			compression = commonmodels.CompressionPgzip
		}
	}

	return commonmodels.DumpedDatabaseSchemaStat{
		DatabaseName:   dbname,
		FileName:       fileName,
		Compression:    compression,
		OriginalSize:   w.GetCount(),
		CompressedSize: r.GetCount(),
	}, nil
}
