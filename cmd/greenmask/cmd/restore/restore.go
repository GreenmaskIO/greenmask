package restore

import (
	"context"
	"fmt"
	"path"

	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"github.com/wwoytenko/greenfuscator/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:  "restore [flags] dumpId|latest",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var dumpId string

			if err := logger.SetLogLevel(Config.Common.LogLevel, Config.Common.LogFormat); err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}

			st, err := directory.NewDirectory(Config.Common.Storage.Directory.Path, 0750, 0650)
			if err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if args[0] == "latest" {
				var backupNames []string

				_, dirs, err := st.ListDir(ctx)
				if err != nil {
					log.Fatal().Err(err).Msg("cannot walk through directory")
				}
				for _, dir := range dirs {
					exists, err := dir.Exists(ctx, "metadata.json")
					if err != nil {
						log.Fatal().Err(err).Msg("cannot check file existence")
					}
					if exists {
						backupNames = append(backupNames, dir.Dirname())
					}
				}

				slices.SortFunc(backupNames, func(a, b string) bool {
					if a > b {
						return true
					}
					return false
				})
				dumpId = backupNames[0]
			} else {
				dumpId = args[0]
				exists, err := st.Exists(ctx, path.Join(dumpId, "metadata.json"))
				if err != nil {
					log.Fatal().Err(err).Msg("cannot check file existence")
				}
				if !exists {
					log.Fatal().Err(err).Msg("choose another dump is failed")
				}
			}

			if err := st.Chdir(ctx, dumpId); err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}

			restore := postgres.NewRestore(Config.Common.BinPath, st, &Config.Restore.PgRestoreOptions, Config.Restore.Scripts)

			log.Info().
				Str("dumpId", dumpId).
				Msgf("restoring dump")
			if err := restore.RunRestore(ctx); err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}
		},
	}
	Config = pgDomains.NewConfig()
)

// TODO: Option that currently does not implemented:
//		* data-only
//  	* exit-on-error
//		* use-list
// 		* schema
// 		* exclude-schema
//		* schema-only
//		* table
// 		* single-transaction
//		* disable-triggers
//		* enable-row-security
//		* no-data-for-failed-tables
//		* section
//		* strict-names

func init() {
	// General options:
	Cmd.Flags().StringP("dbname", "d", "postgres", "connect to database name")
	Cmd.Flags().StringP("file", "f", "", "output file name (- for stdout)")
	Cmd.Flags().StringP("verbose", "v", "", "verbose mode")
	Cmd.Flags().StringP("version", "V", "", "output version information, then exit")

	// Options controlling the output content:
	Cmd.Flags().BoolP("data-only", "a", false, "restore only the data, no schema")
	Cmd.Flags().BoolP("clean", "c", false, "clean (drop) database objects before recreating")
	Cmd.Flags().BoolP("create", "C", false, "create the target database")
	Cmd.Flags().BoolP("exit-on-error", "e", false, "exit on error, default is to continue")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Index, "index", "i", []string{}, "restore named index")
	Cmd.Flags().IntP("jobs", "j", 1, "use this many parallel jobs to restore")
	Cmd.Flags().StringP("list-format", "", "text", "use table of contents in format of text, json or yaml")
	Cmd.Flags().StringP("use-list", "L", "", "use table of contents from this file for selecting/ordering output")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Schema, "schema", "n", []string{}, "restore only objects in this schema")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.ExcludeSchema, "exclude-schema", "N", []string{}, "do not restore objects in this schema")
	Cmd.Flags().StringP("no-owner", "O", "", "skip restoration of object ownership")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Function, "function", "P", []string{}, "restore named function")
	Cmd.Flags().StringP("schema-only", "s", "", "restore only the schema, no data")
	Cmd.Flags().StringP("superuser", "S", "", "superuser user name to use for disabling triggers")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Table, "table", "t", []string{}, "restore named relation (table, view, etc.)")
	Cmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Trigger, "trigger", "T", []string{}, "restore named trigger")
	Cmd.Flags().BoolP("no-privileges", "X", false, "skip restoration of access privileges (grant/revoke)")
	Cmd.Flags().BoolP("single-transaction", "1", false, "restore as a single transaction")
	Cmd.Flags().BoolP("disable-triggers", "", false, "disable triggers during data-only restore")
	Cmd.Flags().BoolP("enable-row-security", "", false, "enable row security")
	Cmd.Flags().BoolP("if-exists", "", false, "use IF EXISTS when dropping objects")
	Cmd.Flags().BoolP("no-comments", "", false, "do not restore comments")
	Cmd.Flags().BoolP("no-data-for-failed-tables", "", false, "do not restore data of tables that could not be created")
	Cmd.Flags().BoolP("no-publications", "", false, "do not restore publications")
	Cmd.Flags().BoolP("no-security-labels", "", false, "do not restore security labels")
	Cmd.Flags().BoolP("no-subscriptions", "", false, "ddo not restore subscriptions")
	Cmd.Flags().BoolP("no-table-access-method", "", false, "do not restore table access methods")
	Cmd.Flags().BoolP("no-tablespaces", "", false, "do not restore tablespace assignments")
	Cmd.Flags().StringP("section", "", "", "restore named section (pre-data, data, or post-data)")
	Cmd.Flags().BoolP("strict-names", "", false, "restore named section (pre-data, data, or post-data) match at least one entity each")
	Cmd.Flags().BoolP("use-set-session-authorization", "", false, "use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership")

	// Connection options:
	Cmd.Flags().StringP("host", "h", "/var/run/postgres", "database server host or socket directory")
	Cmd.Flags().IntP("port", "p", 5432, "database server port number")
	Cmd.Flags().StringP("username", "U", "postgres", "connect as specified database user")

	for _, flagName := range []string{
		"dbname", "file", "verbose", "version",

		"data-only", "clean", "create", "exit-on-error", "jobs", "list-format", "use-list", "schema", "exclude-schema",
		"no-owner", "function", "schema-only", "superuser", "table", "trigger", "no-privileges", "single-transaction",
		"disable-triggers", "enable-row-security", "if-exists", "no-comments", "no-data-for-failed-tables",
		"no-security-labels", "no-subscriptions", "no-table-access-method", "no-tablespaces", "section",
		"strict-names", "use-set-session-authorization",

		"host", "port", "username",
	} {
		flag := Cmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "restore.pg_restore_options", flagName), flag); err != nil {
			log.Fatal().Err(err).Msg("fatal")
		}
	}

	viper.BindEnv("restore.pg_restore_options.dbname", "PGDATABASE")
	viper.BindEnv("restore.pg_restore_options.host", "PGHOST")
	//viper.BindEnv("dbname", "PGOPTIONS")
	viper.BindEnv("restore.pg_restore_options.port", "PGPORT")
	viper.BindEnv("restore.pg_restore_options.username", "PGUSER")
}
