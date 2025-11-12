// Copyright 2023 Greenmask
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
	"os"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/cmdrun"
	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	validateFlags = []cmd.Flag{
		{
			Name:             "table",
			Usage:            "check tables dump only for specific tables",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeStringSlice,
			IsRequired:       false,
			Default:          []string{},
		},
		{
			Name:             "data",
			Usage:            "Run test dump for --rows-limit rows and print it pretty",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "rows-limit",
			Usage:            "Number of rows to dump from each table for validation",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeInt,
			IsRequired:       false,
			Default:          10,
		},
		{
			Name:             "diff",
			Usage:            "Find difference between original and transformed data",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "format",
			Usage:            "Format of the output. Possible values [text|json]",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeString,
			IsRequired:       false,
			Default:          "text",
		},
		{
			Name: "table-format",
			Usage: "Format of the table output (only for --format=text). " +
				"Possible values [vertical|horizontal]",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeString,
			IsRequired:       false,
			Default:          cmdrun.VerticalTableFormat,
		},
		{
			Name:             "transformed-only",
			Usage:            "Print only transformed column and primary key",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "warnings",
			Usage:            "Print warnings",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
		{
			Name:             "schema",
			Usage:            "Make a schema diff between previous dump and the current state",
			ConfigPathPrefix: "validate",
			BindToConfig:     true,
			Type:             cmd.FlagTypeBool,
			IsRequired:       false,
			Default:          false,
		},
	}
	validateCmd = cmd.MustCommand(&cobra.Command{
		Use: "validate",
		Short: "Validate database transformation by performing " +
			"test with limited data dump and print transformation diff",
		Run: func(cmd *cobra.Command, args []string) {
			exitCode, err := cmdrun.RunValidate(rootCmd.MustGetConfig())
			if err != nil {
				log.Fatal(err)
			}
			if exitCode != 0 {
				os.Exit(exitCode)
			}
		},
	}, validateFlags...)
)
