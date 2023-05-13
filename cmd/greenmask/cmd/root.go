package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/delete_backup"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/list_dump"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/restore"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/show_dump"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
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
	log.SetFlags(0)
	cobra.OnInitialize(initConfig)
	// Removing short help flag from default
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for greenmask")
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file ")
	rootCmd.PersistentFlags().StringP("log-format", "", "text", "logging format [plain|text]")
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
		log.Fatal(err)
	}

	if err := viper.BindPFlag("common.log-format", rootCmd.PersistentFlags().Lookup("log-format")); err != nil {
		log.Fatal(err)
	}

	if err := viper.BindPFlag("common.log-level", rootCmd.PersistentFlags().Lookup("log-level")); err != nil {
		log.Fatal(err)
	}

	if err := viper.BindEnv("common.log-level", "LOG_LEVEL"); err != nil {
		log.Fatal(err)
	}
	if err := viper.BindEnv("common.log-format", "LOG_FORMAT"); err != nil {
		log.Fatal(err)
	}

}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
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
		log.Fatalf("unable to read config file: %s", err.Error())
	}

	if err := viper.Unmarshal(&Config); err != nil {
		log.Fatal(err)
	}

}
