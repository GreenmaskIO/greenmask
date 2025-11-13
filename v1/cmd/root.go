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
	rootCmd.AddCommand(restoreCmd)
	rootCmd.AddCommand(listDumpsCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(deleteCmd)
}
