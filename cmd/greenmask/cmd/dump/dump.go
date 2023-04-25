package dump

import (
	"context"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/domains"
	"os"
)

var (
	DumpCmd = &cobra.Command{
		Use: "dump",
		Run: func(cmd *cobra.Command, args []string) {
			pgObfuscator := postgres.NewObfuscator(Config.BinPath, Config.PgDumpOptions)

			if err := pgObfuscator.RunBackup(context.Background(), Config.YamlConfig); err != nil {
				log.Fatal().Err(err).Msg("cannot make a backup")
			}
		},
	}
	cfgFile string
	Config  = &domains.Config{
		PgDumpOptions: &pgdump.Options{},
	}
)

func init() {
	cobra.OnInitialize(initConfig)

	DumpCmd.Flags().StringVar(&cfgFile, "config", "", "config file")

	// pg_dump options

	// General options:
	DumpCmd.Flags().StringP("file", "f", "", "output file or directory name")
	DumpCmd.Flags().StringP("jobs", "j", "", "use this many parallel jobs to dump")
	DumpCmd.Flags().StringP("verbose", "v", "", "verbose mode")
	DumpCmd.Flags().IntP("compress", "Z", 0, "compression level for compressed formats")

	// Connection options
	DumpCmd.Flags().StringP("dbname", "d", "postgres", "database to dump")
	DumpCmd.Flags().StringP("host", "", "/var/run/postgres", "database server host or socket directory")
	DumpCmd.Flags().IntP("port", "p", 5432, "database server port number")
	DumpCmd.Flags().StringP("username", "U", "postgres", "connect as specified database user")
	if err := DumpCmd.MarkFlagRequired("file"); err != nil {
		log.Fatal().Err(err).Msg("fatal")
	}

	for _, flagName := range []string{"file", "jobs", "verbose", "compress", "dbname", "host", "username"} {
		flag := DumpCmd.Flags().Lookup(flagName)
		if err := viper.BindPFlag(flagName, flag); err != nil {
			log.Fatal().Err(err).Msg("fatal")
		}
	}

	viper.SetDefault("dbname", "postgres")
	viper.SetDefault("host", "/var/run/postgres")
	viper.SetDefault("port", 5432)
	viper.SetDefault("username", "postgres")
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
