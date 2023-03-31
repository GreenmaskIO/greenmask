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

package validate

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/greenmaskio/greenmask/internal/db/postgres"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "validate",
		Short: "perform validation procedure and data diff of transformation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			if Config.Common.TempDirectory == "" {
				log.Fatal().Msg("common.tmp_dir cannot be empty")
			}

			if Config.Validate.RowsLimit == 0 {
				log.Fatal().Msgf("--rows-limit must be greater than 0 got %d", Config.Validate.RowsLimit)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			validate, err := postgres.NewValidate(Config, utils.DefaultTransformerRegistry)
			if err != nil {
				log.Fatal().Err(err).Msg("")
			}

			if err := validate.Run(ctx); err != nil {
				log.Fatal().Err(err).Msg("")
			}

		},
	}
	Config = domains.NewConfig()
)

func init() {
	tableFlagName := "table"
	Cmd.Flags().StringSlice(
		tableFlagName, nil, "check tables dump only for specific tables",
	)
	flag := Cmd.Flags().Lookup(tableFlagName)
	if err := viper.BindPFlag("validate.tables", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	dataFlagName := "data"
	Cmd.Flags().Bool(
		dataFlagName, false, "perform test dump for --rows-limit rows and print it pretty",
	)
	flag = Cmd.Flags().Lookup(dataFlagName)
	if err := viper.BindPFlag("validate.data", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	rowsLimitFlagName := "rows-limit"
	Cmd.Flags().Uint64(
		rowsLimitFlagName, 10, "check tables dump only for specific tables",
	)
	flag = Cmd.Flags().Lookup(rowsLimitFlagName)
	if err := viper.BindPFlag("validate.rows_limit", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	diffFlagName := "diff"
	Cmd.Flags().Bool(
		diffFlagName, false, "find difference between original and transformed data",
	)
	flag = Cmd.Flags().Lookup(diffFlagName)
	if err := viper.BindPFlag("validate.diff", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	formatFlagName := "format"
	Cmd.Flags().String(
		formatFlagName, "horizontal", "format of table output. possible values [horizontal|vertical]",
	)
	flag = Cmd.Flags().Lookup(formatFlagName)
	if err := viper.BindPFlag("validate.format", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

}
