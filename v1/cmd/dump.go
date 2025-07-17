package main

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/cmdrun"
	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	dumpFlags = []cmd.Flag{
		{
			Name:             "include-table",
			Usage:            "Include specified table into dump. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "include-schema",
			Usage:            "Include specified schema into dump. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "exclude-table",
			Usage:            "Exclude specified table from dump. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "exclude-schema",
			Usage:            "Exclude specified schema from dump. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "exclude-table-data",
			Usage:            "Dump table structure only, without data. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "data-only",
			Usage:            "Dump data only, without table structure.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "schema-only",
			Usage:            "Dump table structure only, without data.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
	}

	dumpCmd = cmd.MustCommand(&cobra.Command{
		Use:   "dump",
		Short: "Dump database, transform and store into storage.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmdrun.RunDump(rootCmd.MustGetConfig()); err != nil {
				log.Fatal(err)
			}
		},
	}, dumpFlags...)
)
