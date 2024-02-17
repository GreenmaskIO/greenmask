// Copyright 2023 Greenmask
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
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "dump",
		Short: "perform a logical dump, transform data, and store it in storage",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
			if err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}
			st = st.SubStorage(strconv.FormatInt(time.Now().UnixMilli(), 10), true)

			if Config.Common.TempDirectory == "" {
				log.Fatal().Msg("common.tmp_dir cannot be empty")
			}

			dump := cmdInternals.NewDump(Config, st, utils.DefaultTransformerRegistry)

			if err := dump.Run(ctx); err != nil {
				log.Fatal().Err(err).Msg("cannot make a backup")
			}

		},
	}
	Config = pgDomains.NewConfig()
)

// TODO: Check how does work mixed options - use-list + tables, etc.
// TODO: Option that currently does not implemented:
//   - encoding
//   - disable-triggers
//   - lock-wait-timeout
//   - no-sync
//   - data-only
//   - blobs
//   - no-blobs
//   - section
//   - no-synchronized-snapshots
//   - no-unlogged-table-data
//   - strict-names
func init() {
	// General options:
	Cmd.Flags().StringP("file", "f", "", "output file or directory name")
	Cmd.Flags().IntP("jobs", "j", 1, "use this many parallel jobs to dump")
	Cmd.Flags().StringP("verbose", "v", "", "verbose mode")
	Cmd.Flags().IntP("compress", "Z", -1, "compression level for compressed formats")
	Cmd.Flags().IntP("lock-wait-timeout", "", -1, "fail after waiting TIMEOUT for a table lock")
	Cmd.Flags().BoolP("no-sync", "", false, "do not wait for changes to be written safely to dis")

	// Options controlling the output content:
	Cmd.Flags().BoolP("data-only", "a", false, "dump only the data, not the schema")
	Cmd.Flags().BoolP("blobs", "b", false, "include large objects in dump")
	Cmd.Flags().BoolP("no-blobs", "B", false, "exclude large objects in dump")
	Cmd.Flags().BoolP("clean", "c", false, "clean (drop) database objects before recreating")
	Cmd.Flags().BoolP("create", "C", false, "include commands to create database in dump")
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.Extension, "extension", "e", []string{}, "dump the specified extension(s) only",
	)
	Cmd.Flags().StringP("encoding", "E", "", "dump the data in encoding ENCODING")
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.Schema, "schema", "n", []string{}, "dump the specified schema(s) only",
	)
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.ExcludeSchema, "exclude-schema", "N", []string{},
		"dump the specified schema(s) only",
	)
	Cmd.Flags().StringP("no-owner", "O", "", "skip restoration of object ownership in plain-text format")
	Cmd.Flags().StringP("schema-only", "s", "", "dump only the schema, no data")
	Cmd.Flags().StringP("superuser", "S", "", "superuser user name to use in plain-text format")
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.Table, "table", "t", []string{}, "dump the specified table(s) only",
	)
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.ExcludeTable, "exclude-table", "T", []string{}, "do NOT dump the specified table(s)",
	)
	Cmd.Flags().BoolP("no-privileges", "X", false, "do not dump privileges (grant/revoke)")
	Cmd.Flags().BoolP("disable-dollar-quoting", "", false, "disable dollar quoting, use SQL standard quoting")
	Cmd.Flags().BoolP("disable-triggers", "", false, "disable triggers during data-only restore")
	Cmd.Flags().BoolP(
		"enable-row-security", "", false, "enable row security (dump only content user has access to)",
	)
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.ExcludeTableData, "exclude-table-data", "", []string{},
		"do NOT dump data for the specified table(s)",
	)
	Cmd.Flags().StringP("extra-float-digits", "", "", "override default setting for extra_float_digits")
	Cmd.Flags().BoolP("if-exists", "", false, "use IF EXISTS when dropping objects")
	Cmd.Flags().StringSliceVarP(
		&Config.Dump.PgDumpOptions.IncludeForeignData, "include-foreign-data", "", []string{},
		"use IF EXISTS when dropping objects",
	)
	Cmd.Flags().BoolP("load-via-partition-root", "", false, "load partitions via the root table")
	Cmd.Flags().BoolP("no-comments", "", false, "do not dump comments")
	Cmd.Flags().BoolP("no-publications", "", false, "do not dump publications")
	Cmd.Flags().BoolP("no-security-labels", "", false, "do not dump security label assignments")
	Cmd.Flags().BoolP("no-subscriptions", "", false, "do not dump subscriptions")
	Cmd.Flags().BoolP("no-synchronized-snapshots", "", false, "do not use synchronized snapshots in parallel jobs")
	Cmd.Flags().BoolP("no-tablespaces", "", false, "do not dump tablespace assignments")
	Cmd.Flags().BoolP("no-toast-compression", "", false, "do not dump TOAST compression methods")
	Cmd.Flags().BoolP("no-unlogged-table-data", "", false, "do not dump unlogged table data")
	Cmd.Flags().BoolP("on-conflict-do-nothing", "", false, "add ON CONFLICT DO NOTHING to INSERT commands")
	Cmd.Flags().BoolP("quote-all-identifiers", "", false, "quote all identifiers, even if not key words")
	Cmd.Flags().StringP("section", "", "", "dump named section (pre-data, data, or post-data)")
	Cmd.Flags().BoolP("serializable-deferrable", "", false, "wait until the dump can run without anomalies")
	Cmd.Flags().StringP("snapshot", "", "", "use given snapshot for the dump")
	Cmd.Flags().BoolP(
		"strict-names", "", false, "require table and/or schema include patterns to match at least one entity each",
	)
	Cmd.Flags().BoolP(
		"use-set-session-authorization", "", false,
		"use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership",
	)

	// Connection options:
	Cmd.Flags().StringP("dbname", "d", "postgres", "database to dump")
	Cmd.Flags().StringP("host", "h", "/var/run/postgres", "database server host or socket directory")
	Cmd.Flags().IntP("port", "p", 5432, "database server port number")
	Cmd.Flags().StringP("username", "U", "postgres", "connect as specified database user")
	Cmd.Flags().StringP("test", "", "postgres", "connect as specified database user")

	for _, flagName := range []string{
		"file", "jobs", "verbose", "compress", "dbname", "host", "username", "lock-wait-timeout", "no-sync",

		"data-only", "blobs", "no-blobs", "clean", "create", "extension", "encoding", "schema", "exclude-schema",
		"no-owner", "schema-only", "superuser", "table", "exclude-table", "no-privileges", "disable-dollar-quoting",
		"disable-triggers", "enable-row-security", "exclude-table-data", "extra-float-digits", "if-exists",
		"include-foreign-data", "load-via-partition-root", "no-comments", "no-publications", "no-security-labels",
		"no-subscriptions", "no-synchronized-snapshots", "no-tablespaces", "no-toast-compression",
		"no-unlogged-table-data", "on-conflict-do-nothing", "quote-all-identifiers", "section",
		"serializable-deferrable", "snapshot", "strict-names", "use-set-session-authorization",

		"dbname", "host", "port", "username",
	} {
		flag := Cmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "dump.pg_dump_options", flagName), flag); err != nil {
			log.Fatal().Err(err).Msg("")
		}
	}

	if err := viper.BindEnv("dump.pg_dump_options.dbname", "PGDATABASE"); err != nil {
		panic(err)
	}
	if err := viper.BindEnv("dump.pg_dump_options.host", "PGHOST"); err != nil {
		panic(err)
	}
	//viper.BindEnv("dbname", "PGOPTIONS")
	if err := viper.BindEnv("dump.pg_dump_options.port", "PGPORT"); err != nil {
		panic(err)
	}
	if err := viper.BindEnv("dump.pg_dump_options.username", "PGUSER"); err != nil {
		panic(err)
	}
}
