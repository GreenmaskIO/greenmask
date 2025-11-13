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
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/v1/internal/cmdrun"
	"github.com/greenmaskio/greenmask/v1/internal/common/cmd"
)

var (
	pruneFailed  bool
	pruneUnsafe  bool
	dryRun       bool
	retainRecent int
	beforeDate   string
	retainFor    string
)

var (
	deleteFlags = []cmd.Flag{
		{
			Name:         "retain-recent",
			Usage:        "retain the most recent N completed dumps",
			BindToConfig: false,
			Type:         cmd.FlagTypeInt,
			IsRequired:   false,
			Default:      -1,
			Dest:         &retainRecent,
		},
		{
			Name:         "prune-failed",
			Usage:        "prune failed dumps",
			BindToConfig: false,
			Type:         cmd.FlagTypeBool,
			IsRequired:   false,
			Default:      false,
			Dest:         &pruneFailed,
		},
		{
			Name:         "before-date",
			Usage:        "delete dumps older than the specified date in RFC3339 format: 2021-01-01T00:00.0:00Z",
			BindToConfig: false,
			Type:         cmd.FlagTypeString,
			IsRequired:   false,
			Default:      "",
			Dest:         &beforeDate,
		},
		{
			Name:         "retain-for",
			Usage:        "retain dumps for the specified duration in format: 1w2d3h4m5s6ms7us8ns",
			BindToConfig: false,
			Type:         cmd.FlagTypeString,
			IsRequired:   false,
			Default:      "",
			Dest:         &retainFor,
		},
		{
			Name:         "prune-unsafe",
			Usage:        `prune dumps with "unknown-or-failed" statuses. Works only with --prune-failed`,
			BindToConfig: false,
			Type:         cmd.FlagTypeBool,
			IsRequired:   false,
			Default:      false,
			Dest:         &pruneUnsafe,
		},
		{
			Name:         "dry-run",
			Usage:        "do not delete anything, just show what would be deleted",
			BindToConfig: false,
			Type:         cmd.FlagTypeBool,
			IsRequired:   false,
			Default:      false,
			Dest:         &dryRun,
		},
	}
	deleteCmd = cmd.MustCommand(&cobra.Command{
		Use:   "delete",
		Short: "delete dump from the storage with a specific ID",
		Run:   runDelete,
	}, deleteFlags...)
)

func runDelete(_ *cobra.Command, args []string) {
	var dumpId string
	if len(args) > 0 {
		dumpId = args[0]
	}

	if dumpId == "" && retainRecent < 0 && !pruneFailed && beforeDate == "" && retainFor == "" {
		log.Fatal().Msg("at least one deletion criteria must " +
			"be specified: dumpId, --retain-recent, --prune-failed, " +
			"--before-date, --retain-for")
	}

	opts := cmdrun.DeleteOptions{
		DumpID:       dumpId,
		RetainRecent: retainRecent,
		PruneFailed:  pruneFailed,
		BeforeDate:   beforeDate,
		RetainFor:    retainFor,
		PruneUnsafe:  pruneUnsafe,
		DryRun:       dryRun,
	}

	if err := cmdrun.RunDelete(rootCmd.MustGetConfig(), opts); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
