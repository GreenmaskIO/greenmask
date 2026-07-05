// Copyright 2025 Greenmask
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

package main

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/cli"
	"github.com/greenmaskio/greenmask/pkg/common/cmd"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	restoreFlags = []cmd.Flag{
		{
			Name:             "data-only",
			Usage:            "Restore only data, without schema.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "schema-only",
			Usage:            "Restore only schema, without data.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "jobs",
			Usage:            "Number of jobs to use for restoring.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeInt,
			IsRequired:       false,
			Default:          1,
		},
		{
			Name:             "restore-in-order",
			Usage:            "Restore data in order, otherwise the data might not be restored due to dependencies.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "create-database",
			Usage:            "Create databases before restoring schema. Disabled by default.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "if-not-exists",
			Usage:            "Add IF NOT EXISTS to CREATE DATABASE and other object creation statements.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "section",
			Usage:            "Restore only the named section (pre-data, data, post-data). Can be specified multiple times.",
			ConfigPathPrefix: "restore.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
	}

	restoreCmd = cmd.MustCommand(&cobra.Command{
		Use:   "restore [flags] dumpId|latest",
		Args:  cobra.ExactArgs(1),
		Short: "restore dump with ID or the latest to the target database",
		Run: func(cmd *cobra.Command, args []string) {
			cmdRun := cli.New(rootCmd.MustGetConfig())
			if err := cmdRun.Restore(context.Background(), args[0]); err != nil {
				log.Fatal().Err(err).Msg("restore command failed")
			}
		},
	}, restoreFlags...)
)
