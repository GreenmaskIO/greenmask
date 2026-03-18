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
	"log"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/pkg/cmdrun"
	"github.com/greenmaskio/greenmask/pkg/common/cmd"
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
			Name:             "include-database",
			Usage:            "Include specified database into dump. Can be specified multiple times.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "exclude-database",
			Usage:            "Exclude specified database from dump. Can be specified multiple times.",
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
		{
			Name:             "tag",
			Usage:            "Add tag to the dump metadata.",
			ConfigPathPrefix: "dump",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "description",
			Usage:            "Add description to the dump metadata.",
			ConfigPathPrefix: "dump",
			BindToConfig:     true,
			Type:             cmd.FlagTypeString,
			IsRequired:       false,
			Default:          "",
		},
		{
			Name:             "jobs",
			Usage:            "Number of parallel jobs to use for dump.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeInt,
			IsRequired:       false,
			Default:          1,
		},
		{
			Name:             "compress",
			Usage:            "Compress the dump output.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          true,
		},
		{
			Name:             "pgzip",
			Usage:            "Use pgzip for compression.",
			ConfigPathPrefix: "dump.options",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          true,
		},
	}

	dumpCmd = cmd.MustCommand(&cobra.Command{
		Use:   "dump",
		Short: "Dump database, transform and store into storage.",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmdrun.RunDumpCmd(rootCmd.MustGetConfig()); err != nil {
				log.Fatal(err)
			}
		},
	}, dumpFlags...)
)
