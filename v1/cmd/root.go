package main

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	Version string

	rootFlags = []cmd.Flag{
		{
			Name:             "log-format",
			Usage:            "Logging format [text|json]",
			ConfigPathPrefix: "log.format",
			BindToConfig:     true,
			Default:          "text",
		},
		{
			Name: "log-level",
			Usage: fmt.Sprintf(
				"logging level [%s|%s|%s]",
				zerolog.LevelDebugValue,
				zerolog.LevelInfoValue,
				zerolog.LevelWarnValue,
			),
			ConfigPathPrefix: "log.level",
			BindToConfig:     true,
			Default:          zerolog.LevelInfoValue,
		},
	}

	rootCmd = cmd.MustRootCommand(
		&cobra.Command{
			Use:   "greenmask",
			Short: "Greenmask dump and anonymization utility for PostgreSQL, MySQL, etc.",
		},
		getVersion(Version),
		rootFlags...,
	)
)

func init() {
	rootCmd.AddCommand(dumpCmd)
}
