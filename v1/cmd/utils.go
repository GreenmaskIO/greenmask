package main

import (
	"fmt"
	"runtime/debug"
	"strings"

	"github.com/go-viper/mapstructure/v2"
	"github.com/rs/zerolog/log"
	"github.com/spf13/viper"

	"github.com/greenmaskio/greenmask/v1/internal/config"
	cmd2 "github.com/greenmaskio/greenmask/v1/internal/mysql/cmd"
)

func getVersion(version string) string {
	var (
		commitDate string
		commit     string
	)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				cmd2.Commit = setting.Value
			}
			if setting.Key == "vcs.time" {
				cmd2.CommitDate = setting.Value
			}
		}
	}
	if version != "" {
		return fmt.Sprintf("%s %s %s", version, commit, commitDate)
	}
	return fmt.Sprintf("%s %s", commit, commitDate)
}

func initConfig(cfgFile string, cfg *config.Config) error {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil {
			return fmt.Errorf("error reading config file, %s", err)
		}
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			config.ParamsToByteSliceHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
		cfg.ErrorUnused = true
	}

	if err := viper.Unmarshal(cfg, decoderCfg); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if cfgFile != "" {
		// This solves problem with map structure described -> https://github.com/spf13/viper/issues/373
		// that caused issue in Greenmask https://github.com/GreenmaskIO/greenmask/issues/76
		if err := config.ParseTransformerParamsManually(cfgFile, cfg); err != nil {
			log.Fatal().Err(err).Msg("error parsing transformer parameters")
		}
	}
	return nil
}
