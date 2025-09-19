package main

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/cmdrun"
	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	restoreFlags = []cmd.Flag{}

	restoreCmd = cmd.MustCommand(&cobra.Command{
		Use:   "restore [flags] dumpId|latest",
		Args:  cobra.ExactArgs(1),
		Short: "restore dump with ID or the latest to the target database",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmdrun.RunRestore(rootCmd.MustGetConfig(), args[0]); err != nil {
				log.Fatal(err)
			}
		},
	}, restoreFlags...)
)
