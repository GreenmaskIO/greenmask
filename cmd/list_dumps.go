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

	"github.com/greenmaskio/greenmask/pkg/cmdrun"
	"github.com/greenmaskio/greenmask/pkg/common/cmd"
)

var (
	quiet           bool
	listDumpsFormat string
	tags            []string
	statuses        []string
	listDumpsFlags  = []cmd.Flag{
		{
			Name:         "quiet",
			Shorthand:    "q",
			Usage:        "Display only dump IDs",
			BindToConfig: false,
			Type:         cmd.FlagTypeBool,
			Dest:         &quiet,
			Default:      false,
		},
		{
			Name:         "tag",
			Usage:        "Filter dumps by tag",
			BindToConfig: false,
			Type:         cmd.FlagTypeStringSlice,
			IsRequired:   false,
			Default:      []string{},
			Dest:         &tags,
		},
		{
			Name:         "status",
			Usage:        "Filter dumps by status",
			BindToConfig: false,
			Type:         cmd.FlagTypeStringSlice,
			IsRequired:   false,
			Default:      []string{},
			Dest:         &statuses,
		},
		{
			Name:         "format",
			Usage:        "Format of the output. Possible values [text|json]",
			BindToConfig: false,
			Type:         cmd.FlagTypeString,
			IsRequired:   false,
			Default:      "text",
			Dest:         &listDumpsFormat,
		},
	}

	listDumpsCmd = cmd.MustCommand(&cobra.Command{
		Use:   "list-dumps",
		Short: "list all dumps in the storage",
		Run: func(cmd *cobra.Command, args []string) {
			f, err := cmdrun.NewFilter(tags, statuses)
			if err != nil {
				log.Fatal(err)
			}
			fType := cmdrun.OutputFormat(listDumpsFormat)
			if err := fType.Validate(); err != nil {
				log.Fatal(err)
			}
			if err := cmdrun.RunListDumps(rootCmd.MustGetConfig(), quiet, fType, f); err != nil {
				log.Fatal(err)
			}
		},
	}, listDumpsFlags...)
)
