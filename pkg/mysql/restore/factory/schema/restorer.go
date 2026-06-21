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

// Package schema implements the MySQL schema-restore factory for the V2
// registry-based restore pipeline.
package schema

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/connconfig"
)

var _ core.SchemaRestorer = (*MysqlSchemaRestorer)(nil)

// MysqlSchemaPayload is the Payload type stored in SchemaRestoreSpec for MySQL
// schema restore specs. Built by planbuilder.Builder and consumed by this package.
type MysqlSchemaPayload struct {
	Stat      core.SchemaDumpStat
	Databases []string // for CREATE DATABASE, sourced from meta.Databases
}

// MysqlSchemaRestorer restores one (database, section) pair from storage using
// the mysql CLI. Runtime resources (session, conn, storage) are injected at
// Restore time so the restorer is created cheaply by the factory.
type MysqlSchemaRestorer struct {
	cmd     utils.CmdProducer
	payload MysqlSchemaPayload
}

func (r *MysqlSchemaRestorer) DebugInfo() string {
	return fmt.Sprintf("mysql.schema db=%s section=%s", r.payload.Stat.DatabaseName, r.payload.Stat.Section)
}

func (r *MysqlSchemaRestorer) Restore(
	ctx context.Context,
	session core.DatabaseSession,
	conn core.ConnectionConfigurer,
	st core.Storager,
) error {
	cc, ok := conn.ConnectionConfig().(*connconfig.RestoreConnectionConfig)
	if !ok {
		return fmt.Errorf("schema restorer: expected *connconfig.RestoreConnectionConfig, got %T", conn.ConnectionConfig())
	}

	schemaOpts := cc.SchemaRestoreOptions()
	stat := r.payload.Stat

	if schemaOpts.CreateDatabase && stat.Section == core.DumpSectionPreData {
		if err := r.createDatabases(ctx, session, r.payload.Databases, schemaOpts); err != nil {
			return fmt.Errorf("create databases: %w", err)
		}
	}

	params, err := cc.SchemaRestoreParams(schemaOpts.SSL)
	if err != nil {
		return fmt.Errorf("get schema restore params: %w", err)
	}
	env, err := cc.Env()
	if err != nil {
		return fmt.Errorf("get schema restore env: %w", err)
	}
	return r.restoreSchemaFile(ctx, params, env, schemaOpts, st, stat)
}

func (r *MysqlSchemaRestorer) createDatabases(
	ctx context.Context,
	session core.DatabaseSession,
	databases []string,
	opts SchemaRestoreOpts,
) error {
	remap := opts.RemapDatabase
	ifNotExists := opts.IfNotExists

	return session.RunWithOperationalDB(ctx, func(ctx context.Context, db core.DB) error {
		for _, dbName := range databases {
			target := dbName
			if mapped, ok := remap[dbName]; ok {
				target = mapped
			}
			var stmt string
			if ifNotExists {
				stmt = fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", target)
			} else {
				stmt = fmt.Sprintf("CREATE DATABASE `%s`", target)
			}
			log.Ctx(ctx).Debug().Str("database", target).Str("query", stmt).Msg("creating database")
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("create database %q: %w", target, err)
			}
		}
		return nil
	})
}

func (r *MysqlSchemaRestorer) restoreSchemaFile(
	ctx context.Context,
	params []string,
	env []string,
	opts SchemaRestoreOpts,
	st core.Storager,
	stat core.SchemaDumpStat,
) error {
	target := stat.DatabaseName
	if mapped, ok := opts.RemapDatabase[stat.DatabaseName]; ok {
		target = mapped
	}

	log.Ctx(ctx).Info().
		Str("database", stat.DatabaseName).
		Str("target", target).
		Str("section", string(stat.Section)).
		Str("file", stat.FileName).
		Msg("restoring mysql schema")

	f, err := st.GetObject(ctx, stat.FileName)
	if err != nil {
		return fmt.Errorf("get schema file %q: %w", stat.FileName, err)
	}

	var reader io.ReadCloser = f
	if stat.Compression.IsEnabled() {
		reader, err = utils.NewGzipReader(f, stat.Compression.IsPgzip())
		if err != nil {
			return fmt.Errorf("create gzip reader: %w", err)
		}
	}
	defer func() {
		if err := reader.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close schema file")
		}
	}()

	if target != "" {
		params = append(params, target)
	}

	cmd, err := r.cmd.Produce("mysql", params, env, reader)
	if err != nil {
		return fmt.Errorf("produce mysql command: %w", err)
	}
	if err := cmd.ExecuteCmdAndForwardStdout(ctx); err != nil {
		return fmt.Errorf("execute mysql for %q section=%s: %w", stat.DatabaseName, stat.Section, err)
	}
	return nil
}
