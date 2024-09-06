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

package delete

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	gostr "github.com/xhit/go-str2duration/v2"

	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/dumpstatus"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
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
	Cmd = &cobra.Command{
		Use:   "delete",
		Short: "delete dump from the storage with a specific ID",
		//Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			var dumpId string
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("")
			}

			if len(args) > 0 {
				dumpId = args[0]
			}

			if err := run(dumpId); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
	Config = pgDomains.NewConfig()
)

func run(dumpId string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if pruneUnsafe && !pruneFailed {
		log.Fatal().Msg("--include-unsafe works only with --prune-failed")
	}

	if retainFor != "" {
		if err := retainForDumps(ctx, st, retainFor); err != nil {
			log.Fatal().Err(err).Msg("error --retain-for duration")
		}
	} else if retainRecent != -1 {
		if err := retainRecentNDumps(ctx, st); err != nil {
			log.Fatal().
				Err(err).
				Msgf("error retaining the most recent %d dumps", retainRecent)
		}
	} else if pruneFailed {
		if err := pruneFailedDumps(ctx, st, pruneUnsafe); err != nil {
			log.Fatal().Err(err).Msg("error pruning failed dumps")
		}
	} else if beforeDate != "" {
		if err := deleteBeforeDate(ctx, st, beforeDate); err != nil {
			log.Fatal().Err(err).Msg("error deleting dumps elder than date")
		}
	} else if dumpId != "" {
		if err := deleteDump(dumpId); err != nil {
			log.Fatal().Err(err).Msg("error deleting dump")
		}
	} else {
		log.Fatal().Msg("either --prune-failed, --prune-unknown-or-failed, --delete-elder-than, --keep-recent or dumpId should be provided")
	}

	return nil
}

func deleteDump(dumpId string) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if !slices.ContainsFunc(dirs, func(sst storages.Storager) bool {
		return dumpId == sst.Dirname()
	}) {
		return fmt.Errorf("dump with id %s was not found", dumpId)
	}

	if err = st.DeleteAll(ctx, dumpId); err != nil {
		return fmt.Errorf("storage error: %s", err)
	}

	return nil
}

func pruneFailedDumps(ctx context.Context, st storages.Storager, pruneUnsafe bool) error {
	sr, err := getSortedBackupWithStatuses(ctx, st)
	if err != nil {
		return fmt.Errorf("could not get sorted dumps: %s", err)
	}
	for _, d := range sr.Failed {
		if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
			return fmt.Errorf("could not delete dump %s: %s", d.DumpId, err)
		}
	}
	if pruneUnsafe {
		for _, d := range sr.UnknownOrFailed {
			if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
				return fmt.Errorf("could not delete dump %s: %s", d.DumpId, err)
			}
		}
	}
	return nil
}

func deleteBeforeDate(ctx context.Context, st storages.Storager, dateStr string) error {
	dt, err := time.Parse(time.RFC3339Nano, dateStr)
	if err != nil {
		return fmt.Errorf("could not parse --defore-date date: %s", err)
	}
	e := log.Info().
		Bool("DryRun", dryRun).
		Time("BeforeDate", dt)
	if log.Logger.GetLevel() == zerolog.DebugLevel {
		e.Time("BeforeDateUtc", dt.UTC())
	}
	e.Msg("deleting dumps older than")

	sr, err := getSortedBackupWithStatuses(ctx, st)
	if err != nil {
		return fmt.Errorf("could not get sorted dumps: %s", err)
	}
	for _, d := range sr.Valid {
		if d.Date.Before(dt) {
			if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
				return fmt.Errorf("could not delete dump %s: %s", d.DumpId, err)
			}
		}
	}
	return nil
}

func retainForDumps(ctx context.Context, st storages.Storager, retainFor string) error {
	dur, err := gostr.ParseDuration(retainFor)
	if err != nil {
		log.Fatal().Err(err).Msg("error --retain-for duration")
	}
	fromDate := time.Now().Add(-dur)
	log.Info().
		Bool("DryRun", dryRun).
		Str("Duration", gostr.String(dur)).
		Time("ToDate", time.Now()).
		Time("FromDate", fromDate).
		Msg("deleting dumps older than")

	sr, err := getSortedBackupWithStatuses(ctx, st)
	if err != nil {
		return fmt.Errorf("could not get sorted dumps: %s", err)
	}
	for _, d := range sr.Valid {
		if time.Since(d.Date) < dur {
			continue
		}
		if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
			return fmt.Errorf("could not delete dump %s: %s", d.DumpId, err)
		}
	}
	return nil
}

func retainRecentNDumps(ctx context.Context, st storages.Storager) error {
	sr, err := getSortedBackupWithStatuses(ctx, st)
	if err != nil {
		return fmt.Errorf("could not get sorted dumps: %s", err)
	}

	log.Info().
		Int("Kept", retainRecent).
		Bool("DryRun", dryRun).
		Msg("retaining the most recent N dumps")

	for idx, d := range sr.Valid {
		if idx < retainRecent {
			continue
		}
		if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
			return fmt.Errorf("could not delete dump %s: %s", d.DumpId, err)
		}
	}
	return nil
}

func getSortedBackupWithStatuses(ctx context.Context, st storages.Storager) (*StorageResponse, error) {
	var valid, failed, unknownOrFailed []*Dump
	_, backups, err := st.ListDir(ctx)
	if err != nil {
		return nil, err
	}
	for _, backup := range backups {
		status, md, err := dumpstatus.GetDumpStatusAndMetadata(ctx, backup)
		if err != nil {
			log.Warn().
				Str("DumpId", backup.Dirname()).
				Err(err).
				Msg("unable to get dump status for dump")
		}
		d := Dump{
			DumpId: backup.Dirname(),
			Status: status,
		}
		if status == dumpstatus.DoneStatusName {
			d.Date = md.StartedAt
			d.Database = md.Header.DbName
		}
		switch status {
		case dumpstatus.DoneStatusName:
			valid = append(valid, &d)
		case dumpstatus.FailedStatusName:
			failed = append(failed, &d)
		case dumpstatus.UnknownOrFailedStatusName:
			unknownOrFailed = append(unknownOrFailed, &d)
		}
	}

	slices.SortFunc(valid, func(a, b *Dump) int {
		return cmp.Compare(b.DumpId, a.DumpId)
	})

	slices.SortFunc(failed, func(a, b *Dump) int {
		return cmp.Compare(b.DumpId, a.DumpId)
	})

	slices.SortFunc(unknownOrFailed, func(a, b *Dump) int {
		return cmp.Compare(b.DumpId, a.DumpId)
	})

	return &StorageResponse{
		Valid:           valid,
		Failed:          failed,
		UnknownOrFailed: unknownOrFailed,
	}, nil
}

func deleteDumpById(ctx context.Context, st storages.Storager, d *Dump, dryRun bool) error {
	if d.DumpId == "" {
		panic("empty dump id")
	}
	e := log.Info().
		Str("DumpId", d.DumpId)
	if !d.Date.IsZero() {
		e.Str("Date", d.Date.String())
	}
	if log.Logger.GetLevel() == zerolog.DebugLevel {
		e.Str("DateUTC", d.Date.UTC().String())
	}
	if d.Database != "" {
		e.Str("Database", d.Database)
	}
	msg := "deleting dump"
	if dryRun {
		msg = "deleting dump (dry-run)"
	}
	e.Msg(msg)

	if dryRun {
		return nil
	}
	if err := st.DeleteAll(ctx, d.DumpId); err != nil {
		return err
	}
	return nil
}

func init() {
	// General options:
	Cmd.Flags().IntVar(&retainRecent,
		"retain-recent",
		-1,
		"retain the most recent N completed dumps",
	)
	Cmd.Flags().BoolVar(&pruneFailed,
		"prune-failed",
		false,
		"prune failed dumps",
	)
	Cmd.Flags().StringVar(&beforeDate,
		"before-date",
		"",
		"delete dumps older than the specified date in RFC3339Nano format: 2021-01-01T00:00.0:00Z",
	)
	Cmd.Flags().StringVar(&retainFor,
		"retain-for",
		"",
		"retain dumps for the specified duration in format: 1w2d3h4m5s6ms7us8ns",
	)
	Cmd.Flags().BoolVar(&pruneUnsafe,
		"prune-unsafe",
		false,
		`prune dumps with "unknown-or-failed" statuses. Works only with --prune-failed`,
	)
	Cmd.Flags().BoolVar(&dryRun,
		"dry-run",
		false,
		"do not delete anything, just show what would be deleted",
	)
}
