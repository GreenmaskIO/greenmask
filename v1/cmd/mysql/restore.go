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

package mysql

import (
	"context"
	"fmt"
	"path"
	"slices"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/mysql"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

const (
	metadataJsonFileName = "metadata.json"
)

var (
	restoreCmd = &cobra.Command{
		Use:   "restore [flags] dumpId|latest",
		Args:  cobra.ExactArgs(1),
		Short: "restore dump with ID or the latest to the target database",
		Run: func(cmd *cobra.Command, args []string) {

			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
			if err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}

			dumpId, err := getDumpId(ctx, st, args[0])
			if err != nil {
				log.Fatal().Err(err).Msg("")
			}

			st = st.SubStorage(dumpId, true)

			restore := cmdInternals.NewRestore()

			log.Info().
				Str("dumpId", dumpId).
				Msgf("restoring dump")
			if err := restore.Run(ctx); err != nil {
				log.Fatal().Err(err).Msg("fatal")
			}
		},
	}
)

func getDumpId(ctx context.Context, st storages.Storager, dumpId string) (string, error) {
	if dumpId == latestDumpName {
		var backupNames []string

		_, dirs, err := st.ListDir(ctx)
		if err != nil {
			log.Fatal().Err(err).Msg("cannot walk through directory")
		}
		for _, dir := range dirs {
			exists, err := dir.Exists(ctx, metadataJsonFileName)
			if err != nil {
				log.Fatal().Err(err).Msg("cannot check file existence")
			}
			if exists {
				backupNames = append(backupNames, dir.Dirname())
			}
		}

		slices.SortFunc(
			backupNames, func(a, b string) int {
				if a > b {
					return -1
				}
				return 1
			},
		)
		dumpId = backupNames[0]
	} else {
		exists, err := st.Exists(ctx, path.Join(dumpId, metadataJsonFileName))
		if err != nil {
			log.Fatal().
				Err(err).
				Msg("cannot check file existence")
		}
		if !exists {
			log.Fatal().
				Err(err).
				Str("DumpId", dumpId).
				Msg("dump with provided id is not found")
		}
	}
	return dumpId, nil
}

func init() {
	// General options:
	restoreCmd.Flags().StringP("dbname", "d", "postgres", "connect to database name")
	restoreCmd.Flags().StringP("file", "f", "", "output file name (- for stdout)")
	restoreCmd.Flags().StringP("verbose", "v", "", "verbose mode")

	for _, flagName := range []string{
		"dbname", "file", "verbose",
	} {
		flag := restoreCmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "restore.pg_restore_options", flagName), flag); err != nil {
			log.Fatal().Err(err).Msg("fatal")
		}
	}

	//if err := viper.BindEnv("restore.pg_restore_options.dbname", "PGDATABASE"); err != nil {
	//	panic(err)
	//}
	//if err := viper.BindEnv("restore.pg_restore_options.host", "PGHOST"); err != nil {
	//	panic(err)
	//}
	//if err := viper.BindEnv("restore.pg_restore_options.port", "PGPORT"); err != nil {
	//	panic(err)
	//}
	//if err := viper.BindEnv("restore.pg_restore_options.username", "PGUSER"); err != nil {
	//	panic(err)
	//}
}
