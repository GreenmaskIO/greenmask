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

package cmd

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/delete_backup"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/dump"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/list_dumps"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/list_transformers"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/restore"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/show_dump"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/show_transformer"
	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd/validate"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	configUtils "github.com/greenmaskio/greenmask/internal/utils/config"
)

var (
	Version    string
	Commit     string
	CommitDate string

	RootCmd = &cobra.Command{
		Use:   "greenmask",
		Short: "Greenmask is a stateless logical dump tool with features for obfuscaction",
		Long: "A useful and flexible logical backup tool that works with pg_dump directory " +
			"format and keep backward compatibility with pg_restore. It allows make an obfuscation " +
			"procedure with dumping tables on the fly. It provides declarative config for your " +
			"backup and possibility to implement your own obfuscation features using custom " +
			"transformers. Supports a few storages (directory and S3)",
		//DisableFlagParsing: true,
	}
	cfgFile string
	Config  = pgDomains.NewConfig()
)

func Execute() error {
	return RootCmd.Execute()
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				Commit = setting.Value
			}
			if setting.Key == "vcs.time" {
				CommitDate = setting.Value
			}
		}
	}
	if Version != "" {
		RootCmd.Version = fmt.Sprintf("%s %s %s", Version, Commit, CommitDate)
	} else {
		RootCmd.Version = fmt.Sprintf("%s %s", Commit, CommitDate)
	}

	cobra.OnInitialize(initConfig)
	// Removing short help flag from default
	RootCmd.PersistentFlags().BoolP("help", "", false, "help for greenmask")
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file ")
	RootCmd.PersistentFlags().StringP("log-format", "", "text", "logging format [text|json]")
	RootCmd.PersistentFlags().StringP("log-level", "", zerolog.LevelInfoValue,
		fmt.Sprintf(
			"logging level %s|%s|%s",
			zerolog.LevelDebugValue,
			zerolog.LevelInfoValue,
			zerolog.LevelWarnValue,
		),
	)

	RootCmd.AddCommand(dump.Cmd)
	RootCmd.AddCommand(list_dumps.Cmd)
	RootCmd.AddCommand(restore.Cmd)
	RootCmd.AddCommand(delete_backup.Cmd)
	RootCmd.AddCommand(show_dump.Cmd)
	RootCmd.AddCommand(list_transformers.Cmd)
	RootCmd.AddCommand(validate.Cmd)
	RootCmd.AddCommand(show_transformer.Cmd)

	if err := viper.BindPFlag("log.format", RootCmd.PersistentFlags().Lookup("log-format")); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if err := viper.BindPFlag("log.level", RootCmd.PersistentFlags().Lookup("log-level")); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	RootCmd.InitDefaultCompletionCmd()
	RootCmd.InitDefaultHelpCmd()
	RootCmd.InitDefaultVersionFlag()

	for _, c := range RootCmd.Commands() {
		if c.Name() == "completion" || c.Name() == "help" {
			c.DisableFlagParsing = true
			for _, subc := range c.Commands() {
				subc.DisableFlagParsing = true
			}
		}
	}

}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			log.Fatal().Err(err).Msg("error reading from config file")
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			configUtils.ParamsToByteSliceHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
	}

	if err := viper.Unmarshal(Config, decoderCfg); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
