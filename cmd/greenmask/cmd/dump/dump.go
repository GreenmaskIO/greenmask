package dump

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"github.com/wwoytenko/greenfuscator/internal/utils/logger"
)

var (
	DumpCmd = &cobra.Command{
		Use: "dump",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Common.LogLevel, Config.Common.LogFormat); err != nil {
				log.Fatal(err)
			}

			rootSt, err := directory.NewDirectory(Config.Common.Storage.Directory.Path, 0750, 0650)
			if err != nil {
				log.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			st, err := rootSt.CreateDir(ctx, strconv.FormatInt(time.Now().UnixMilli(), 10))
			if err != nil {
				log.Fatalf("cannot create directory in storage: %s", err)
			}
			dump := postgres.NewDump(Config.Common.BinPath, &Config.Dump.PgDumpOptions, st, Config.Dump.Transformers)

			if Config.Dump.PgDumpOptions.Validate {
				if err := postgres.RunValidate(ctx, &Config.Dump.PgDumpOptions, Config.Dump.Transformers); err != nil {
					log.Fatalf("validation error: %s", err)
				}
			} else {
				if err := dump.RunDump(ctx); err != nil {
					log.Fatalf("cannot make a backup: %s", err)
				}
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
	DumpCmd.Flags().StringP("file", "f", "", "output file or directory name")
	DumpCmd.Flags().IntP("jobs", "j", 1, "use this many parallel jobs to dump")
	DumpCmd.Flags().StringP("verbose", "v", "", "verbose mode")
	DumpCmd.Flags().IntP("compress", "Z", -1, "compression level for compressed formats")
	DumpCmd.Flags().IntP("lock-wait-timeout", "", -1, "fail after waiting TIMEOUT for a table lock")
	DumpCmd.Flags().BoolP("no-sync", "", false, "do not wait for changes to be written safely to dis")

	// Options controlling the output content:
	DumpCmd.Flags().BoolP("data-only", "a", false, "dump only the data, not the schema")
	DumpCmd.Flags().BoolP("blobs", "b", false, "include large objects in dump")
	DumpCmd.Flags().BoolP("no-blobs", "B", false, "exclude large objects in dump")
	DumpCmd.Flags().BoolP("clean", "c", false, "clean (drop) database objects before recreating")
	DumpCmd.Flags().BoolP("create", "C", false, "include commands to create database in dump")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.Extension, "extension", "e", []string{}, "dump the specified extension(s) only")
	DumpCmd.Flags().StringP("encoding", "E", "", "dump the data in encoding ENCODING")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.Schema, "schema", "n", []string{}, "dump the specified schema(s) only")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.ExcludeSchema, "exclude-schema", "N", []string{}, "dump the specified schema(s) only")
	DumpCmd.Flags().StringP("no-owner", "O", "", "skip restoration of object ownership in plain-text format")
	DumpCmd.Flags().StringP("schema-only", "s", "", "dump only the schema, no data")
	DumpCmd.Flags().StringP("superuser", "S", "", "superuser user name to use in plain-text format")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.Table, "table", "t", []string{}, "dump the specified table(s) only")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.ExcludeTable, "exclude-table", "T", []string{}, "do NOT dump the specified table(s)")
	DumpCmd.Flags().BoolP("no-privileges", "X", false, "do not dump privileges (grant/revoke)")
	DumpCmd.Flags().BoolP("disable-dollar-quoting", "", false, "disable dollar quoting, use SQL standard quoting")
	DumpCmd.Flags().BoolP("disable-triggers", "", false, "disable triggers during data-only restore")
	DumpCmd.Flags().BoolP("enable-row-security", "", false, "enable row security (dump only content user has access to)")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.ExcludeTableData, "exclude-table-data", "", []string{}, "do NOT dump data for the specified table(s)")
	DumpCmd.Flags().StringP("extra-float-digits", "", "", "override default setting for extra_float_digits")
	DumpCmd.Flags().BoolP("if-exists", "", false, "use IF EXISTS when dropping objects")
	DumpCmd.Flags().StringSliceVarP(&Config.Dump.PgDumpOptions.IncludeForeignData, "include-foreign-data", "", []string{}, "use IF EXISTS when dropping objects")
	DumpCmd.Flags().BoolP("load-via-partition-root", "", false, "load partitions via the root table")
	DumpCmd.Flags().BoolP("no-comments", "", false, "do not dump comments")
	DumpCmd.Flags().BoolP("no-publications", "", false, "do not dump publications")
	DumpCmd.Flags().BoolP("no-security-labels", "", false, "do not dump security label assignments")
	DumpCmd.Flags().BoolP("no-subscriptions", "", false, "do not dump subscriptions")
	DumpCmd.Flags().BoolP("no-synchronized-snapshots", "", false, "do not use synchronized snapshots in parallel jobs")
	DumpCmd.Flags().BoolP("no-tablespaces", "", false, "do not dump tablespace assignments")
	DumpCmd.Flags().BoolP("no-toast-compression", "", false, "do not dump TOAST compression methods")
	DumpCmd.Flags().BoolP("no-unlogged-table-data", "", false, "do not dump unlogged table data")
	DumpCmd.Flags().BoolP("on-conflict-do-nothing", "", false, "add ON CONFLICT DO NOTHING to INSERT commands")
	DumpCmd.Flags().BoolP("quote-all-identifiers", "", false, "quote all identifiers, even if not key words")
	DumpCmd.Flags().StringP("section", "", "", "dump named section (pre-data, data, or post-data)")
	DumpCmd.Flags().BoolP("serializable-deferrable", "", false, "wait until the dump can run without anomalies")
	DumpCmd.Flags().StringP("snapshot", "", "", "use given snapshot for the dump")
	DumpCmd.Flags().BoolP("strict-names", "", false, "require table and/or schema include patterns to match at least one entity each")
	DumpCmd.Flags().BoolP("use-set-session-authorization", "", false, "use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership")

	// Connection options:
	DumpCmd.Flags().StringP("dbname", "d", "postgres", "database to dump")
	DumpCmd.Flags().StringP("host", "h", "/var/run/postgres", "database server host or socket directory")
	DumpCmd.Flags().IntP("port", "p", 5432, "database server port number")
	DumpCmd.Flags().StringP("username", "U", "postgres", "connect as specified database user")
	DumpCmd.Flags().StringP("test", "", "postgres", "connect as specified database user")

	DumpCmd.Flags().BoolP("validate", "", false, "validate config")

	for _, flagName := range []string{
		"file", "jobs", "verbose", "compress", "dbname", "host", "username", "lock-wait-timeout", "no-sync",

		"data-only", "blobs", "no-blobs", "clean", "create", "extension", "encoding", "schema", "exclude-schema",
		"no-owner", "schema-only", "superuser", "table", "exclude-table", "no-privileges", "disable-dollar-quoting",
		"disable-triggers", "enable-row-security", "exclude-table-data", "extra-float-digits", "if-exists",
		"include-foreign-data", "load-via-partition-root", "no-comments", "no-publications", "no-security-labels",
		"no-subscriptions", "no-synchronized-snapshots", "no-tablespaces", "no-toast-compression",
		"no-unlogged-table-data", "on-conflict-do-nothing", "quote-all-identifiers", "section",
		"serializable-deferrable", "snapshot", "strict-names", "use-set-session-authorization",

		"dbname", "host", "port", "username", "validate",
	} {
		flag := DumpCmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "dump.pg_dump_options", flagName), flag); err != nil {
			log.Fatal(err)
		}
	}

	viper.BindEnv("dump.pg_dump_options.dbname", "PGDATABASE")
	viper.BindEnv("dump.pg_dump_options.host", "PGHOST")
	//viper.BindEnv("dbname", "PGOPTIONS")
	viper.BindEnv("dump.pg_dump_options.port", "PGPORT")
	viper.BindEnv("dump.pg_dump_options.username", "PGUSER")
}
