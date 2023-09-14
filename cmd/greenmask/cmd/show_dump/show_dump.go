package show_dump

import (
	"context"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/rs/zerolog/log"
	"path"
	"slices"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/internal/db/postgres"
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

			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("error setting up logger")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
			if err != nil {
				log.Fatal().Err(err).Msg("error building storage")
			}

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

				slices.SortFunc(backupNames, func(a, b string) int {
					if a > b {
						return -1
					}
					return 1
				})
				dumpId = backupNames[0]
			} else {
				dumpId = args[0]
				exists, err := st.Exists(ctx, path.Join(dumpId, "metadata.json"))
				if err != nil {
					log.Fatal().Err(err).Msg("cannot check file existence")
				}
				if !exists {
					log.Fatal().Msgf("choose another dump %s is failed", dumpId)
				}
			}

			if err := postgres.ShowDump(ctx, st, dumpId, format); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
)

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", "text", "output format [text|yaml|json]")
}
