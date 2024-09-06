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

package show_dump

import (
	"context"
	"path"
	"slices"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

const (
	latestDumpName = "latest"
)

var (
	Config = pgDomains.NewConfig()
	format string
)

var (
	Cmd = &cobra.Command{
		Use:   "show-dump [flags] dumpId|latest",
		Args:  cobra.ExactArgs(1),
		Short: "shows metadata info about the dump (the same as pg_restore -l ./)",
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

			if args[0] == latestDumpName {
				var backupNames []string

				_, dirs, err := st.ListDir(ctx)
				if err != nil {
					log.Fatal().Err(err).Msg("cannot walk through directory")
				}
				for _, dir := range dirs {
					exists, err := dir.Exists(ctx, cmdInternals.MetadataJsonFileName)
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
				exists, err := st.Exists(ctx, path.Join(dumpId, cmdInternals.MetadataJsonFileName))
				if err != nil {
					log.Fatal().Err(err).Msg("cannot check file existence")
				}
				if !exists {
					log.Fatal().Msgf("choose another dump %s is failed", dumpId)
				}
			}

			if err := cmdInternals.ShowDump(ctx, st, dumpId, format); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
)

func init() {
	Cmd.Flags().StringVarP(&format, "format", "f", "text", "output format [text|yaml|json]")
}
