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
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/validate"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "validate",
		Short: "perform validation procedure and data diff of transformation",
		Run:   run,
	}
	Config = domains.NewConfig()
)

func run(cmd *cobra.Command, args []string) {
	if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
		log.Err(err).Msg("")
	}

	if Config.Common.TempDirectory == "" {
		log.Fatal().Msg("common.tmp_dir cannot be empty")
	}

	if Config.Validate.RowsLimit <= 0 {
		log.Fatal().
			Msgf("--rows-limit must be greater than 0 got %d", Config.Validate.RowsLimit)
	}

	if Config.Validate.Format != cmdInternals.JsonFormat &&
		Config.Validate.Format != cmdInternals.TextFormat {
		log.Fatal().
			Str("RequestedFormat", Config.Validate.Format).
			Msg("unknown --format value")
	}

	if Config.Validate.TableFormat != cmdInternals.VerticalTableFormat &&
		Config.Validate.TableFormat != cmdInternals.HorizontalTableFormat {
		log.Fatal().
			Str("RequestedTableFormat", Config.Validate.TableFormat).
			Msg("unknown --table-format value")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	validateCmd, err := cmdInternals.NewValidate(Config, utils.DefaultTransformerRegistry, validate.New(""))
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	exitCode, err := validateCmd.Run(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	if exitCode != 0 {
		os.Exit(exitCode)
	}
}

func init() {
	tableFlagName := "table"
	Cmd.Flags().StringSlice(
		tableFlagName, nil, "Check tables dump only for specific tables",
	)
	flag := Cmd.Flags().Lookup(tableFlagName)
	if err := viper.BindPFlag("validate.tables", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	dataFlagName := "data"
	Cmd.Flags().Bool(
		dataFlagName, false, "Perform test dump for --rows-limit rows and print it pretty",
	)
	flag = Cmd.Flags().Lookup(dataFlagName)
	if err := viper.BindPFlag("validate.data", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	rowsLimitFlagName := "rows-limit"
	Cmd.Flags().Uint64(
		rowsLimitFlagName, 10, "Check tables dump only for specific tables",
	)
	flag = Cmd.Flags().Lookup(rowsLimitFlagName)
	if err := viper.BindPFlag("validate.rows_limit", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	diffFlagName := "diff"
	Cmd.Flags().Bool(
		diffFlagName, false, "Find difference between original and transformed data",
	)
	flag = Cmd.Flags().Lookup(diffFlagName)
	if err := viper.BindPFlag("validate.diff", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	formatFlagName := "format"
	Cmd.Flags().String(
		formatFlagName, "text", "Format of output. possible values [text|json]",
	)
	flag = Cmd.Flags().Lookup(formatFlagName)
	if err := viper.BindPFlag("validate.format", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	tableFormatFlagName := "table-format"
	Cmd.Flags().String(
		tableFormatFlagName, cmdInternals.VerticalTableFormat, "Format of table output (only for --format=text). Possible values [vertical|horizontal]",
	)
	flag = Cmd.Flags().Lookup(tableFormatFlagName)
	if err := viper.BindPFlag("validate.table_format", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	onlyTransformedFlagName := "transformed-only"
	Cmd.Flags().Bool(
		onlyTransformedFlagName, false, "Print only transformed column and primary key",
	)
	flag = Cmd.Flags().Lookup(onlyTransformedFlagName)
	if err := viper.BindPFlag("validate.transformed_only", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	warningsFlagName := "warnings"
	Cmd.Flags().Bool(
		warningsFlagName, false, "Print warnings",
	)
	flag = Cmd.Flags().Lookup(warningsFlagName)
	if err := viper.BindPFlag("validate.warnings", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	schemaFlagName := "schema"
	Cmd.Flags().Bool(
		schemaFlagName, false, "Make a schema diff between previous dump and the current state",
	)
	flag = Cmd.Flags().Lookup(schemaFlagName)
	if err := viper.BindPFlag("validate.schema", flag); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

}
