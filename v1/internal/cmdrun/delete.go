package cmdrun

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gostr "github.com/xhit/go-str2duration/v2"

	"github.com/greenmaskio/greenmask/v1/internal/common/heartbeat"
	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

var ErrDeleteWrongOptions = errors.New("wrong delete options")

type DeleteMode string

const (
	DeleteModePruneFailed  DeleteMode = "prune-failed"
	DeleteModeBeforeDate   DeleteMode = "before-date"
	DeleteModeRetainRecent DeleteMode = "retain-recent"
	DeleteModeRetainFor    DeleteMode = "retain-for"
	DeleteModeDumpID       DeleteMode = "dump-id"
)

type DumpStatus string

const (
	DumpStatusDone            DumpStatus = "done"
	DumpStatusInProgress      DumpStatus = "in-progress"
	DumpStatusFailed          DumpStatus = "failed"
	DumpStatusUnknownOrFailed DumpStatus = "unknown-or-failed"
)

type StorageResponse struct {
	Valid           []*DumpInfo
	Failed          []*DumpInfo
	UnknownOrFailed []*DumpInfo
}

type DumpInfo struct {
	DumpID   string
	Date     time.Time
	Status   DumpStatus
	Database string
}

type DeleteOptions struct {
	PruneFailed  bool
	PruneUnsafe  bool
	DryRun       bool
	RetainRecent int
	BeforeDate   string
	RetainFor    string
	DumpID       string
}

func (o *DeleteOptions) GetMode() (DeleteMode, error) {
	switch {
	case o.RetainFor != "":
		return DeleteModeRetainFor, nil
	case o.RetainRecent != -1:
		return DeleteModeRetainRecent, nil
	case o.PruneFailed:
		return DeleteModePruneFailed, nil
	case o.BeforeDate != "":
		return DeleteModeBeforeDate, nil
	case o.DumpID != "":
		return DeleteModeDumpID, nil
	default:
		return "", ErrDeleteWrongOptions
	}
}

func (o *DeleteOptions) Validate() error {
	if o.PruneUnsafe && !o.PruneFailed {
		return fmt.Errorf("--include-unsafe works only with --prune-failed")
	}
	return nil
}

func RunDelete(cfg *config.Config, opts DeleteOptions) error {
	ctx := context.Background()
	ctx = setupContext(ctx, cfg)
	st, err := commonutils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	if err := opts.Validate(); err != nil {
		return err
	}
	mode, err := opts.GetMode()
	if err != nil {
		return fmt.Errorf("get delete mode: %w", err)
	}
	switch mode {
	case DeleteModeRetainFor:
		if err := retainForDumps(ctx, st, opts.RetainFor, opts.DryRun); err != nil {
			return fmt.Errorf("retain for dumps: %w", err)
		}
	case DeleteModeRetainRecent:
		if err := retainRecentNDumps(ctx, st, opts.RetainRecent, opts.DryRun); err != nil {
			return fmt.Errorf("retain recent dumps: %w", err)
		}
	case DeleteModePruneFailed:
		if err := pruneFailedDumps(ctx, st, opts.PruneUnsafe, opts.DryRun); err != nil {
			return fmt.Errorf("prune failed dumps: %w", err)
		}
	case DeleteModeBeforeDate:
		if err := deleteBeforeDate(ctx, st, opts.BeforeDate, opts.DryRun); err != nil {
			return fmt.Errorf("deleting dumps elder than date: %w", err)
		}
	case DeleteModeDumpID:
		if err := deleteSingleDump(ctx, st, opts.DumpID, opts.DryRun); err != nil {
			return fmt.Errorf("delete dump by id: %w", err)
		}
	default:
		return fmt.Errorf("unknown delete mode '%s': %w", mode, commonmodels.ErrValueValidationFailed)
	}

	return nil
}

func deleteSingleDump(
	ctx context.Context,
	st interfaces.Storager,
	dumpID string,
	dryRun bool,
) error {
	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if !slices.ContainsFunc(dirs, func(sst interfaces.Storager) bool {
		return dumpID == sst.Dirname()
	}) {
		return fmt.Errorf("dump with id '%s' is not found: %w", dumpID, commonmodels.ErrFatalError)
	}

	e := log.Info().
		Str("DumpID", dumpID)
	msg := "deleting dump"
	if dryRun {
		msg = "deleting dump (dry-run)"
	}
	e.Msg(msg)

	if dryRun {
		return nil
	}

	if err = st.DeleteAll(ctx, dumpID); err != nil {
		return fmt.Errorf("storage error: %s", err)
	}

	return nil
}

func pruneFailedDumps(
	ctx context.Context,
	st interfaces.Storager,
	pruneUnsafe bool,
	dryRun bool,
) error {
	sr, err := getSortedBackupWithStatuses(ctx, st)
	if err != nil {
		return fmt.Errorf("could not get sorted dumps: %s", err)
	}
	for _, d := range sr.Failed {
		if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
			return fmt.Errorf("could not delete dump %s: %s", d.DumpID, err)
		}
	}
	if pruneUnsafe {
		for _, d := range sr.UnknownOrFailed {
			if err = deleteDumpById(ctx, st, d, dryRun); err != nil {
				return fmt.Errorf("could not delete dump %s: %s", d.DumpID, err)
			}
		}
	}
	return nil
}

func deleteBeforeDate(
	ctx context.Context,
	st interfaces.Storager,
	dateStr string,
	dryRun bool,
) error {
	dt, err := time.Parse(time.RFC3339, dateStr)
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
				return fmt.Errorf("could not delete dump %s: %s", d.DumpID, err)
			}
		}
	}
	return nil
}

func retainForDumps(
	ctx context.Context,
	st interfaces.Storager,
	retainFor string,
	dryRun bool,
) error {
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
			return fmt.Errorf("could not delete dump %s: %s", d.DumpID, err)
		}
	}
	return nil
}

func retainRecentNDumps(
	ctx context.Context,
	st interfaces.Storager,
	retainRecent int,
	dryRun bool,
) error {
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
			return fmt.Errorf("could not delete dump %s: %s", d.DumpID, err)
		}
	}
	return nil
}

func heartbeatStatusToDumpStatus(hbStatus heartbeat.Status) DumpStatus {
	switch hbStatus {
	case heartbeat.StatusDone:
		return DumpStatusDone
	case heartbeat.StatusInProgress:
		return DumpStatusInProgress
	case heartbeat.StatusFailed:
		return DumpStatusFailed
	default:
		return DumpStatusUnknownOrFailed
	}
}

func getSortedBackupWithStatuses(ctx context.Context, st interfaces.Storager) (*StorageResponse, error) {
	var valid, failed, unknownOrFailed []*DumpInfo
	_, backups, err := st.ListDir(ctx)
	if err != nil {
		return nil, err
	}
	for _, backup := range backups {
		status, metadata, err := getMetadataAndStatus(ctx, backup)
		if err != nil {
			log.Warn().
				Str("DumpID", backup.Dirname()).
				Err(fmt.Errorf("unable to get dump status for dump: %w", err)).
				Msg("considering dump as unknown-or-failed")
		}
		dumpStatus := heartbeatStatusToDumpStatus(status)
		d := DumpInfo{
			DumpID: backup.Dirname(),
			Status: dumpStatus,
		}
		if status == heartbeat.StatusDone {
			d.Date = metadata.StartedAt
			d.Database = metadata.DatabaseName
		}
		switch dumpStatus {
		case DumpStatusDone:
			valid = append(valid, &d)
		case DumpStatusFailed:
			failed = append(failed, &d)
		case DumpStatusUnknownOrFailed:
			unknownOrFailed = append(unknownOrFailed, &d)
		default:
			panic(fmt.Sprintf("unhandled status '%s'", status))
		}
	}

	slices.SortFunc(valid, func(a, b *DumpInfo) int {
		return cmp.Compare(b.DumpID, a.DumpID)
	})

	slices.SortFunc(failed, func(a, b *DumpInfo) int {
		return cmp.Compare(b.DumpID, a.DumpID)
	})

	slices.SortFunc(unknownOrFailed, func(a, b *DumpInfo) int {
		return cmp.Compare(b.DumpID, a.DumpID)
	})

	return &StorageResponse{
		Valid:           valid,
		Failed:          failed,
		UnknownOrFailed: unknownOrFailed,
	}, nil
}

func deleteDumpById(ctx context.Context, st interfaces.Storager, d *DumpInfo, dryRun bool) error {
	if d.DumpID == "" {
		panic("empty dump id")
	}
	e := log.Info().
		Str("DumpID", d.DumpID)
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
	if err := st.DeleteAll(ctx, d.DumpID); err != nil {
		return err
	}
	return nil
}
