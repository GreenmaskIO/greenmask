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

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

const (
	executable = "mysql"
)

type options interface {
	SchemaRestoreParams() ([]string, error)
	Env() ([]string, error)
}

// Restorer - restores mysql schema from the given storage folder.
// It expected that schema DDL commands are stored in schema.sql.
type Restorer struct {
	st         interfaces.Storager
	cfg        options
	executable string
	cmd        utils.CmdProducer
	schemaMeta *commonmodels.SchemaDumpMetadata
}

func NewRestorer(
	st interfaces.Storager,
	connCfg options,
	cmd utils.CmdProducer,
	schemaMeta *commonmodels.SchemaDumpMetadata,
) *Restorer {
	return &Restorer{
		st:         st,
		cfg:        connCfg,
		executable: executable,
		cmd:        cmd,
		schemaMeta: schemaMeta,
	}
}

func (r *Restorer) restoreSchemaData(ctx context.Context, dbName string, f io.Reader) error {
	params, err := r.cfg.SchemaRestoreParams()
	if err != nil {
		return fmt.Errorf("get schema restore params: %w", err)
	}

	if dbName != "" {
		params = append(params, dbName)
	}

	env, err := r.cfg.Env()
	if err != nil {
		return fmt.Errorf("get schema restore env: %w", err)
	}
	cmd, err := r.cmd.Produce(r.executable, params, env, f)
	if err != nil {
		return fmt.Errorf("produce schema restore command: %w", err)
	}

	if err := cmd.ExecuteCmdAndForwardStdout(ctx); err != nil {
		return fmt.Errorf("execute schema restore command: %w", err)
	}
	return nil
}

func (r *Restorer) RestoreSchema(ctx context.Context) error {
	if r.schemaMeta == nil {
		log.Ctx(ctx).Debug().Msg("no schema dump found in metadata")
		return nil
	}

	for _, schemaStat := range r.schemaMeta.DumpedDatabaseSchema {
		if err := r.restoreDatabaseSchema(ctx, schemaStat); err != nil {
			return fmt.Errorf("database '%s': %w", schemaStat.DatabaseName, err)
		}
	}
	return nil
}

func (r *Restorer) restoreDatabaseSchema(ctx context.Context, schemaStat commonmodels.DumpedDatabaseSchemaStat) error {
	log.Ctx(ctx).Info().
		Str("Database", schemaStat.DatabaseName).
		Str("FileName", schemaStat.FileName).
		Msg("restoring database schema")

	f, err := r.st.GetObject(ctx, schemaStat.FileName)
	if err != nil {
		return fmt.Errorf("get schema file from storage: %w", err)
	}

	reader := f
	if schemaStat.Compression != commonmodels.CompressionNone {
		gzReader, err := utils.NewGzipReader(f, schemaStat.Compression == commonmodels.CompressionPgzip)
		if err != nil {
			return fmt.Errorf("create gzip reader: %w", err)
		}
		reader = gzReader
	}

	defer func() {
		if err := reader.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close schema file")
		}
	}()

	if err := r.restoreSchemaData(ctx, schemaStat.DatabaseName, reader); err != nil {
		return fmt.Errorf("restore schema data: %w", err)
	}
	return nil
}
