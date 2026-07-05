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
	"database/sql"
	"fmt"
	"io"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

const (
	executable = "mysql"
)

type options interface {
	SchemaRestoreParams(ssl commonconfig.SSLOpts) ([]string, error)
	Env() ([]string, error)
}

type Option func(*Restorer)

// WithCreateDatabase instructs the restorer to issue CREATE DATABASE statements
// before restoring the pre-data schema.
func WithCreateDatabase(conn *sql.DB, databases []string) Option {
	return func(r *Restorer) {
		r.conn = conn
		r.databases = databases
		r.createDatabase = true
	}
}

// WithIfNotExists adds IF NOT EXISTS to CREATE DATABASE statements.
// Has no effect unless WithCreateDatabase is also applied.
func WithIfNotExists() Option {
	return func(r *Restorer) {
		r.ifNotExists = true
	}
}

// WithDatabaseRemap sets the database name mapping applied before schema creation and restoration.
func WithDatabaseRemap(remap map[string]string) Option {
	return func(r *Restorer) {
		r.databaseRemap = remap
	}
}

func (r *Restorer) remapDB(name string) string {
	if mapped, ok := r.databaseRemap[name]; ok {
		return mapped
	}
	return name
}

// Restorer restores a MySQL schema from files stored in the dump directory.
type Restorer struct {
	st             core.Storager
	cfg            options
	sslOpts        commonconfig.SSLOpts
	executable     string
	cmd            utils.CmdProducer
	schemaMeta     *core.SchemaDumpMetadata
	conn           *sql.DB
	databases      []string
	createDatabase bool
	ifNotExists    bool
	databaseRemap  map[string]string
}

func NewRestorer(
	st core.Storager,
	connCfg options,
	sslOpts commonconfig.SSLOpts,
	cmd utils.CmdProducer,
	schemaMeta *core.SchemaDumpMetadata,
	opts ...Option,
) *Restorer {
	r := &Restorer{
		st:         st,
		cfg:        connCfg,
		sslOpts:    sslOpts,
		executable: executable,
		cmd:        cmd,
		schemaMeta: schemaMeta,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Restorer) restoreSchemaData(ctx context.Context, dbName string, f io.Reader) error {
	params, err := r.cfg.SchemaRestoreParams(r.sslOpts)
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

func (r *Restorer) createDatabases(ctx context.Context) error {
	for _, db := range r.databases {
		target := r.remapDB(db)
		var stmt string
		if r.ifNotExists {
			stmt = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", target)
		} else {
			stmt = fmt.Sprintf("CREATE DATABASE `%s`", target)
		}
		log.Ctx(ctx).Debug().
			Str("DatabaseName", target).
			Str("Query", stmt).
			Msg("creating database")
		if _, err := r.conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("create database %q: %w", target, err)
		}
	}
	return nil
}

// RestorePreDataSchema restores the pre-data section (tables, views) for all databases.
// For backward-compatible dumps that have no section set, the file is treated as pre-data.
func (r *Restorer) RestorePreDataSchema(ctx context.Context) error {
	if r.schemaMeta == nil {
		log.Ctx(ctx).Debug().Msg("no schema dump found in metadata")
		return nil
	}

	if r.createDatabase && len(r.databases) > 0 {
		if err := r.createDatabases(ctx); err != nil {
			return fmt.Errorf("create databases: %w", err)
		}
	}

	for _, schemaStat := range r.schemaMeta.DumpedDatabaseSchema {
		// Backward compat: entries without a section are treated as pre-data.
		if schemaStat.Section != "" && schemaStat.Section != core.DumpSectionPreData {
			continue
		}
		if err := r.restoreDatabaseSchema(ctx, schemaStat); err != nil {
			return fmt.Errorf("database '%s': %w", schemaStat.DatabaseName, err)
		}
	}
	return nil
}

// RestorePostDataSchema restores the post-data section (triggers, routines, events) for all databases.
func (r *Restorer) RestorePostDataSchema(ctx context.Context) error {
	if r.schemaMeta == nil {
		log.Ctx(ctx).Debug().Msg("no schema dump found in metadata")
		return nil
	}

	for _, schemaStat := range r.schemaMeta.DumpedDatabaseSchema {
		if schemaStat.Section != core.DumpSectionPostData {
			continue
		}
		if err := r.restoreDatabaseSchema(ctx, schemaStat); err != nil {
			return fmt.Errorf("database '%s': %w", schemaStat.DatabaseName, err)
		}
	}
	return nil
}

func (r *Restorer) restoreDatabaseSchema(ctx context.Context, schemaStat core.SchemaDumpStat) error {
	target := r.remapDB(schemaStat.DatabaseName)
	log.Ctx(ctx).Info().
		Str("Database", schemaStat.DatabaseName).
		Str("TargetDatabase", target).
		Str("Section", string(schemaStat.Section)).
		Str("FileName", schemaStat.FileName).
		Msg("restoring database schema")

	f, err := r.st.GetObject(ctx, schemaStat.FileName)
	if err != nil {
		return fmt.Errorf("get schema file from storage: %w", err)
	}

	reader := f
	if schemaStat.Compression.IsEnabled() {
		gzReader, err := utils.NewGzipReader(f, schemaStat.Compression.IsPgzip())
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

	if err := r.restoreSchemaData(ctx, target, reader); err != nil {
		return fmt.Errorf("restore schema data: %w", err)
	}
	return nil
}
