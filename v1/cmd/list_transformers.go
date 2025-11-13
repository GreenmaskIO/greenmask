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

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/cmdrun"
	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	format string

	listTransformersFlags = []cmd.Flag{
		{
			Name:             "format",
			Usage:            "output format [text|json]",
			ConfigPathPrefix: "",
			BindToConfig:     false,
			Type:             cmd.FlagTypeString,
			IsRequired:       false,
			Default:          "text",
			Dest:             &format,
		},
	}

	listTransformersCmd = cmd.MustCommand(&cobra.Command{
		Use:   "list-transformers",
		Short: "list of the allowed transformers with documentation",
		Run: func(cmd *cobra.Command, args []string) {
			if err := cmdrun.RunListTransformers(rootCmd.MustGetConfig(), cmdrun.OutputFormat(format)); err != nil {
				log.Fatal(err)
			}
		},
	}, listTransformersFlags...)
)
