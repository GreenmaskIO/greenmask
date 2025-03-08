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
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/mysql"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

var (
	dumpCmd = &cobra.Command{
		Use:   "dump",
		Short: "perform a logical dump, transform data, and store it in storage",
		Run:   run,
	}
)

func run(cmd *cobra.Command, args []string) {
	if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
		log.Fatal().
			Err(err).
			Msg("setup loger")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := storages.GetStorage(ctx, Config.Storage, Config.Log)
	if err != nil {
		log.Fatal().
			Err(err).
			Msg("get storage")
	}
	st = st.SubStorage(strconv.FormatInt(time.Now().UnixMilli(), 10), true)

	validateConfig()

	//dump := cmdInternals.NewDump(&Config.Dump, st, "mysqldump")
	dump := common.NewDumpRuntime()

	if err := dump.Run(ctx); err != nil {
		log.Fatal().
			Err(err).
			Msg("cannot make a backup")
	}
}

func validateConfig() {
	if Config.Common.TempDirectory == "" {
		log.Fatal().
			Msg("common.tmp_dir cannot be empty")
	}
}

func init() {
	// General options:
	dumpCmd.Flags().StringP("file", "f", "", "output file or directory name")

	for _, flagName := range []string{
		//"file", "jobs", "verbose", "compress", "dbname", "host", "username", "lock-wait-timeout", "no-sync",
	} {
		flag := dumpCmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(fmt.Sprintf("%s.%s", "dump.options", flagName), flag); err != nil {
			log.Fatal().Err(err).Msg("")
		}
	}

	//if err := viper.BindEnv("dump.pg_dump_options.dbname", "PGDATABASE"); err != nil {
	//	panic(err)
	//}
	//if err := viper.BindEnv("dump.pg_dump_options.host", "PGHOST"); err != nil {
	//	panic(err)
	//}
	////viper.BindEnv("dbname", "PGOPTIONS")
	//if err := viper.BindEnv("dump.pg_dump_options.port", "PGPORT"); err != nil {
	//	panic(err)
	//}
	//if err := viper.BindEnv("dump.pg_dump_options.username", "PGUSER"); err != nil {
	//	panic(err)
	//}
}
