package show_dump

import (
	"context"
	"log"
	"path"

	"github.com/spf13/cobra"
	"golang.org/x/exp/slices"

	"github.com/greenmaskio/greenmask/internal/db/postgres"
	pgDomains "github.com/greenmaskio/greenmask/internal/db/postgres/domains/config"
	"github.com/greenmaskio/greenmask/internal/storages/directory"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Config = pgDomains.NewConfig()
	format string
)

var (
	Cmd = &cobra.Command{
		Use:   "show-dump [flags] dumpId|latest",
		Args:  cobra.ExactArgs(1),
		Short: "Print archive meta information (the same as pg_restore -l ./)",
		Run: func(cmd *cobra.Command, args []string) {
			var dumpId string

			if err := logger.SetLogLevel(Config.Common.LogLevel, Config.Common.LogFormat); err != nil {
				log.Fatal(err)
			}

			st, err := directory.NewDirectory(Config.Common.Storage.Directory.Path, 0750, 0650)
			if err != nil {
				log.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if args[0] == "latest" {
				var backupNames []string

				_, dirs, err := st.ListDir(ctx)
				if err != nil {
					log.Fatalf("cannot walk through directory: %s", err)
				}
				for _, dir := range dirs {
					exists, err := dir.Exists(ctx, "metadata.json")
					if err != nil {
						log.Fatalf("cannot check file existence: %s", err)
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
					log.Fatalf("cannot check file existence: %s", err)
				}
				if !exists {
					log.Fatalf("choose another dump %s is failed", dumpId)
				}
			}

			if err := postgres.ShowDump(ctx, st, dumpId, format); err != nil {
				log.Fatal(err)
			}
		},
	}
)

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", "text", "output format [text|yaml|json]")
}
