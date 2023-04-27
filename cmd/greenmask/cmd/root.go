package cmd

import (
	"github.com/spf13/cobra"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
)

var (
	cfgFile     string
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
)

func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Removing short help flag from default
	rootCmd.PersistentFlags().BoolP("help", "", false, "help for greenmask")
	rootCmd.AddCommand(dump.DumpCmd)
	rootCmd.AddCommand(dump.ListDumpCmd)
}
