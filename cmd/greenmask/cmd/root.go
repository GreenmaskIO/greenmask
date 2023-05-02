package cmd

import (
	"fmt"
	"os"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
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
	cobra.OnInitialize(initConfig)
	// Removing short help flag from default
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for greenmask")
	rootCmd.AddCommand(dump.DumpCmd)
	rootCmd.AddCommand(dump.ListDumpCmd)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file")

	if err := rootCmd.MarkPersistentFlagRequired("config"); err != nil {
		log.Fatal().Err(err).Msg("fatal")
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

	// Why is here err == nil ?
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	if err := viper.Unmarshal(&Config); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

}
