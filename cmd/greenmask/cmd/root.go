package cmd

import (
	"fmt"
	"os"

	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/GreenmaskIO/greenmask/cmd/greenmask/cmd/delete_backup"
	"github.com/GreenmaskIO/greenmask/cmd/greenmask/cmd/dump"
	"github.com/GreenmaskIO/greenmask/cmd/greenmask/cmd/list_dump"
	"github.com/GreenmaskIO/greenmask/cmd/greenmask/cmd/restore"
	"github.com/GreenmaskIO/greenmask/cmd/greenmask/cmd/show_dump"
	pgDomains "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/config"
)

var (
	userLicence string
	rootCmd     = &cobra.Command{
		Use:   "greenmask",
		Short: "Greenmask is a stateless logical dump tool with features for obfuscaction",
		Long: `A useful and flexible logical backup tool that works with pg_dump directory
format and keep backward compatibility with pg_restore. It allows make an obfuscation 
procedure with dumping tables on the fly. It provides declarative config for your 
backup and possibility to implement your own obfuscation features using custom 
transformers. Supports a few storages (directoris and S3)`,
	}
	cfgFile string
	Config  = pgDomains.NewConfig()
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	cobra.OnInitialize(initConfig)
	// Removing short help flag from default
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for greenmask")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file ")
	rootCmd.PersistentFlags().StringP("log-format", "", "text", "logging format [text|json]")
	rootCmd.PersistentFlags().StringP("log-level", "", zerolog.LevelInfoValue,
		fmt.Sprintf(
			"logging level %s|%s|%s",
			zerolog.LevelDebugValue,
			zerolog.LevelInfoValue,
			zerolog.LevelWarnValue,
		),
	)

	rootCmd.AddCommand(dump.DumpCmd)
	rootCmd.AddCommand(list_dump.Cmd)
	rootCmd.AddCommand(restore.Cmd)
	rootCmd.AddCommand(delete_backup.Cmd)
	rootCmd.AddCommand(show_dump.Cmd)

	if err := rootCmd.MarkPersistentFlagRequired("config"); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if err := viper.BindPFlag("common.log-format", rootCmd.PersistentFlags().Lookup("log-format")); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if err := viper.BindPFlag("common.log-level", rootCmd.PersistentFlags().Lookup("log-level")); err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if err := viper.BindEnv("common.log-level", "LOG_LEVEL"); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	if err := viper.BindEnv("common.log-format", "LOG_FORMAT"); err != nil {
		log.Fatal().Err(err).Msg("")
	}

}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
		//f, err := os.Open(cfgFile)
		//if err != nil {
		//	log.Fatal().Err(err).Msg("")
		//}
		//defer f.Close()
		//if err := yaml.NewDecoder(f).Decode(&Config); err != nil {
		//	log.Fatal().Err(err).Msg("")
		//}
	} else {
		home, err := os.UserConfigDir()
		cobra.CheckErr(err)

		viper.AddConfigPath(home)
		viper.SetConfigType("yaml")
		viper.SetConfigType("yml")
		viper.SetConfigName(".greenmask")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		log.Fatal().Msgf("unable to read config file: %s", err.Error())
	}

	decoderCfg := func(cfg *mapstructure.DecoderConfig) {
		cfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
			pgDomains.ParamsToByteSliceHookFunc(),
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
		)
		log.Debug().Any("decoderCfg", cfg).Msg("")
	}

	if err := viper.Unmarshal(&Config, decoderCfg); err != nil {
		log.Fatal().Err(err).Msg("")
	}

}
