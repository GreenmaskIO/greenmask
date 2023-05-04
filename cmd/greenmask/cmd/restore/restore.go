package restore

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres"
)

var (
	RestoreCmd = &cobra.Command{
		Use:  "restore [flags] dumpId",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			st, err := directory.NewDirectory(Config.Common.Storage.Directory.Path, 0750, 0650)
			if err != nil {
				log.Fatal(err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			restore := postgres.NewRestore(Config.Common.BinPath, st)

			if err := restore.RunRestore(ctx, &Config.Restore.PgRestoreOptions, args[0]); err != nil {
				log.Fatal(err)
			}
		},
	}
	Config = pgDomains.NewConfig()
)

func init() {
	// General options:
	RestoreCmd.Flags().StringP("dbname", "d", "postgres", "connect to database name")
	RestoreCmd.Flags().StringP("file", "f", "", "output file name (- for stdout)")
	RestoreCmd.Flags().BoolP("list", "l", false, "print summarized TOC of the archive")
	RestoreCmd.Flags().StringP("verbose", "v", "", "verbose mode")
	RestoreCmd.Flags().StringP("version", "V", "", "output version information, then exit")

	// Options controlling the output content:
	RestoreCmd.Flags().BoolP("data-only", "a", false, "restore only the data, no schema")
	RestoreCmd.Flags().BoolP("clean", "c", false, "clean (drop) database objects before recreating")
	RestoreCmd.Flags().BoolP("create", "C", false, "create the target database")
	RestoreCmd.Flags().BoolP("exit-on-error", "e", false, "exit on error, default is to continue")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Index, "index", "i", []string{}, "restore named index")
	RestoreCmd.Flags().IntP("jobs", "j", 1, "use this many parallel jobs to restore")
	RestoreCmd.Flags().StringP("use-list", "L", "", "use table of contents from this file for selecting/ordering output")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Schema, "schema", "n", []string{}, "restore only objects in this schema")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.ExcludeSchema, "exclude-schema", "N", []string{}, "do not restore objects in this schema")
	RestoreCmd.Flags().StringP("no-owner", "O", "", "skip restoration of object ownership")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Function, "function", "P", []string{}, "restore named function")
	RestoreCmd.Flags().StringP("schema-only", "s", "", "restore only the schema, no data")
	RestoreCmd.Flags().StringP("superuser", "S", "", "superuser user name to use for disabling triggers")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Table, "table", "t", []string{}, "restore named relation (table, view, etc.)")
	RestoreCmd.Flags().StringSliceVarP(&Config.Restore.PgRestoreOptions.Trigger, "trigger", "T", []string{}, "restore named trigger")
	RestoreCmd.Flags().BoolP("no-privileges", "X", false, "skip restoration of access privileges (grant/revoke)")
	RestoreCmd.Flags().BoolP("single-transaction", "1", false, "restore as a single transaction")
	RestoreCmd.Flags().BoolP("disable-triggers", "", false, "disable triggers during data-only restore")
	RestoreCmd.Flags().BoolP("enable-row-security", "", false, "enable row security")
	RestoreCmd.Flags().BoolP("if-exists", "", false, "use IF EXISTS when dropping objects")
	RestoreCmd.Flags().BoolP("no-comments", "", false, "do not restore comments")
	RestoreCmd.Flags().BoolP("no-data-for-failed-tables", "", false, "do not restore data of tables that could not be created")
	RestoreCmd.Flags().BoolP("no-publications", "", false, "do not restore publications")
	RestoreCmd.Flags().BoolP("no-security-labels", "", false, "do not restore security labels")
	RestoreCmd.Flags().BoolP("no-subscriptions", "", false, "ddo not restore subscriptions")
	RestoreCmd.Flags().BoolP("no-table-access-method", "", false, "do not restore table access methods")
	RestoreCmd.Flags().BoolP("no-tablespaces", "", false, "do not restore tablespace assignments")
	RestoreCmd.Flags().StringP("section", "", "", "restore named section (pre-data, data, or post-data)")
	RestoreCmd.Flags().BoolP("strict-names", "", false, "restore named section (pre-data, data, or post-data) match at least one entity each")
	RestoreCmd.Flags().BoolP("use-set-session-authorization", "", false, "use SET SESSION AUTHORIZATION commands instead of ALTER OWNER commands to set ownership")

	// Connection options:
	RestoreCmd.Flags().StringP("host", "h", "/var/run/postgres", "database server host or socket directory")
	RestoreCmd.Flags().IntP("port", "p", 5432, "database server port number")
	RestoreCmd.Flags().StringP("username", "U", "postgres", "connect as specified database user")

	for _, flagName := range []string{
		"dbname", "file", "list", "verbose", "version",

		"data-only", "clean", "create", "exit-on-error", "jobs", "use-list", "schema", "exclude-schema",
		"no-owner", "function", "schema-only", "superuser", "table", "trigger", "no-privileges", "single-transaction",
		"disable-triggers", "enable-row-security", "if-exists", "no-comments", "no-data-for-failed-tables",
		"no-security-labels", "no-subscriptions", "no-table-access-method", "no-tablespaces", "section",
		"strict-names", "use-set-session-authorization",

		"host", "port", "username",
	} {
		flag := RestoreCmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "restore.pg_restore_options", flagName), flag); err != nil {
			log.Fatal(err)
		}
	}

	viper.BindEnv("restore.pg_restore_options.dbname", "PGDATABASE")
	viper.BindEnv("restore.pg_restore_options.host", "PGHOST")
	//viper.BindEnv("dbname", "PGOPTIONS")
	viper.BindEnv("restore.pg_restore_options.port", "PGPORT")
	viper.BindEnv("restore.pg_restore_options.username", "PGUSER")
}
