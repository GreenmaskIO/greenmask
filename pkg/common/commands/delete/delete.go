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

package delete

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	gostr "github.com/xhit/go-str2duration/v2"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
)

const metadataFileName = "metadata.json"

// DumpStatus is the operational status of a stored dump.
type DumpStatus string

const (
	DumpStatusDone            DumpStatus = "done"
	DumpStatusInProgress      DumpStatus = "in-progress"
	DumpStatusFailed          DumpStatus = "failed"
	DumpStatusUnknownOrFailed DumpStatus = "unknown-or-failed"
)

// DumpInfo holds the identity and key metadata of a single dump.
type DumpInfo struct {
	DumpID   string
	Date     time.Time
	Status   DumpStatus
	Database string
}

// storageResponse groups dumps by status after scanning the storage root.
// In-progress dumps are omitted (skipped by the scanner).
type storageResponse struct {
	Valid           []*DumpInfo
	Failed          []*DumpInfo
	UnknownOrFailed []*DumpInfo
}

// Deleter executes targeted delete operations against dump storage.
type Deleter struct {
	st                core.Storager
	heartbeatInterval time.Duration
}

// New returns a Deleter backed by the given storage root. heartbeatInterval
// is the stale-timeout forwarded to the heartbeat reader.
func New(st core.Storager, heartbeatInterval time.Duration) *Deleter {
	return &Deleter{st: st, heartbeatInterval: heartbeatInterval}
}

// ByDumpID deletes the single dump identified by dumpID. Returns an error
// when the dump does not exist in storage.
func (d *Deleter) ByDumpID(ctx context.Context, dumpID string, dryRun bool) error {
	_, dirs, err := d.st.ListDir(ctx)
	if err != nil {
		return fmt.Errorf("list storage directory: %w", err)
	}
	if !slices.ContainsFunc(dirs, func(s core.Storager) bool {
		return s.Dirname() == dumpID
	}) {
		return fmt.Errorf("dump with id %q is not found: %w", dumpID, core.ErrFatalError)
	}
	e := log.Ctx(ctx).Info().Str("DumpID", dumpID)
	if dryRun {
		e.Msg("deleting dump (dry-run)")
		return nil
	}
	e.Msg("deleting dump")
	if err := d.st.DeleteAll(ctx, dumpID); err != nil {
		return fmt.Errorf("storage error: %w", err)
	}
	return nil
}

// PruneFailed deletes all dumps whose status is "failed". When pruneUnsafe is
// true, dumps with unknown-or-failed status are also deleted.
func (d *Deleter) PruneFailed(ctx context.Context, pruneUnsafe bool, dryRun bool) error {
	sr, err := d.getSortedDumps(ctx)
	if err != nil {
		return fmt.Errorf("get sorted dumps: %w", err)
	}
	for _, info := range sr.Failed {
		if err := d.deleteByInfo(ctx, info, dryRun); err != nil {
			return fmt.Errorf("delete dump %s: %w", info.DumpID, err)
		}
	}
	if pruneUnsafe {
		for _, info := range sr.UnknownOrFailed {
			if err := d.deleteByInfo(ctx, info, dryRun); err != nil {
				return fmt.Errorf("delete dump %s: %w", info.DumpID, err)
			}
		}
	}
	return nil
}

// BeforeDate deletes all successful dumps started before the given RFC3339
// date string.
func (d *Deleter) BeforeDate(ctx context.Context, dateStr string, dryRun bool) error {
	dt, err := time.Parse(time.RFC3339, dateStr)
	if err != nil {
		return fmt.Errorf("parse date %q: %w", dateStr, err)
	}
	e := log.Ctx(ctx).Info().Bool("DryRun", dryRun).Time("BeforeDate", dt)
	if log.Logger.GetLevel() == zerolog.DebugLevel {
		e.Time("BeforeDateUtc", dt.UTC())
	}
	e.Msg("deleting dumps older than")

	sr, err := d.getSortedDumps(ctx)
	if err != nil {
		return fmt.Errorf("get sorted dumps: %w", err)
	}
	for _, info := range sr.Valid {
		if info.Date.Before(dt) {
			if err := d.deleteByInfo(ctx, info, dryRun); err != nil {
				return fmt.Errorf("delete dump %s: %w", info.DumpID, err)
			}
		}
	}
	return nil
}

// RetainFor deletes all successful dumps older than the given duration string
// (e.g. "7d", "24h", "2w").
func (d *Deleter) RetainFor(ctx context.Context, retainFor string, dryRun bool) error {
	dur, err := gostr.ParseDuration(retainFor)
	if err != nil {
		return fmt.Errorf("parse duration %q: %w", retainFor, err)
	}
	fromDate := time.Now().Add(-dur)
	log.Ctx(ctx).Info().
		Bool("DryRun", dryRun).
		Str("Duration", gostr.String(dur)).
		Time("ToDate", time.Now()).
		Time("FromDate", fromDate).
		Msg("deleting dumps older than")

	sr, err := d.getSortedDumps(ctx)
	if err != nil {
		return fmt.Errorf("get sorted dumps: %w", err)
	}
	for _, info := range sr.Valid {
		if time.Since(info.Date) < dur {
			continue
		}
		if err := d.deleteByInfo(ctx, info, dryRun); err != nil {
			return fmt.Errorf("delete dump %s: %w", info.DumpID, err)
		}
	}
	return nil
}

// RetainRecent keeps the n most recent successful dumps and deletes the rest.
func (d *Deleter) RetainRecent(ctx context.Context, n int, dryRun bool) error {
	sr, err := d.getSortedDumps(ctx)
	if err != nil {
		return fmt.Errorf("get sorted dumps: %w", err)
	}
	log.Ctx(ctx).Info().
		Int("Kept", n).
		Bool("DryRun", dryRun).
		Msg("retaining the most recent N dumps")

	for idx, info := range sr.Valid {
		if idx < n {
			continue
		}
		if err := d.deleteByInfo(ctx, info, dryRun); err != nil {
			return fmt.Errorf("delete dump %s: %w", info.DumpID, err)
		}
	}
	return nil
}

// getSortedDumps scans the storage root and groups dumps by status.
// Each slice is sorted newest-first by dump ID. In-progress dumps are skipped.
func (d *Deleter) getSortedDumps(ctx context.Context) (*storageResponse, error) {
	_, backups, err := d.st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("list storage directory: %w", err)
	}

	var valid, failed, unknownOrFailed []*DumpInfo
	for _, backup := range backups {
		info, err := d.readDumpInfo(ctx, backup)
		if err != nil {
			log.Ctx(ctx).Warn().
				Str("DumpID", backup.Dirname()).
				Err(fmt.Errorf("unable to get dump status: %w", err)).
				Msg("considering dump as unknown-or-failed")
			info = &DumpInfo{DumpID: backup.Dirname(), Status: DumpStatusUnknownOrFailed}
		}
		switch info.Status {
		case DumpStatusDone:
			valid = append(valid, info)
		case DumpStatusFailed:
			failed = append(failed, info)
		case DumpStatusUnknownOrFailed:
			unknownOrFailed = append(unknownOrFailed, info)
		case DumpStatusInProgress:
			log.Ctx(ctx).Debug().
				Str("DumpID", backup.Dirname()).
				Msg("dump status is in progress: skipping in delete operations")
		}
	}

	slices.SortFunc(valid, func(a, b *DumpInfo) int { return cmp.Compare(b.DumpID, a.DumpID) })
	slices.SortFunc(failed, func(a, b *DumpInfo) int { return cmp.Compare(b.DumpID, a.DumpID) })
	slices.SortFunc(unknownOrFailed, func(a, b *DumpInfo) int { return cmp.Compare(b.DumpID, a.DumpID) })

	return &storageResponse{
		Valid:           valid,
		Failed:          failed,
		UnknownOrFailed: unknownOrFailed,
	}, nil
}

// readDumpInfo reads heartbeat and metadata for a single dump directory.
// When heartbeat says "done" but metadata.json is missing, the dump is
// treated as failed (consistent with the list-dumps behaviour).
func (d *Deleter) readDumpInfo(ctx context.Context, backup core.Storager) (*DumpInfo, error) {
	hbStatus, err := heartbeat.NewReader(backup).SetStaleTimeout(d.heartbeatInterval).Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("read heartbeat: %w", err)
	}
	info := &DumpInfo{
		DumpID: backup.Dirname(),
		Status: toStatus(hbStatus),
	}
	if hbStatus != heartbeat.StatusDone {
		return info, nil
	}
	md, err := readMetadata(ctx, backup)
	if err != nil {
		if errors.Is(err, core.ErrFileNotFound) {
			info.Status = DumpStatusFailed
			return info, nil
		}
		return nil, fmt.Errorf("read metadata: %w", err)
	}
	info.Date = md.StartedAt
	info.Database = strings.Join(md.Databases, ", ")
	return info, nil
}

func readMetadata(ctx context.Context, st core.Storager) (core.Metadata, error) {
	r, err := st.GetObject(ctx, metadataFileName)
	if err != nil {
		return core.Metadata{}, err
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close metadata file")
		}
	}()
	var md core.Metadata
	if err := json.NewDecoder(r).Decode(&md); err != nil {
		return core.Metadata{}, fmt.Errorf("decode metadata: %w", err)
	}
	return md, nil
}

func (d *Deleter) deleteByInfo(ctx context.Context, info *DumpInfo, dryRun bool) error {
	e := log.Ctx(ctx).Info().Str("DumpID", info.DumpID)
	if !info.Date.IsZero() {
		e.Str("Date", info.Date.String())
		if log.Logger.GetLevel() == zerolog.DebugLevel {
			e.Str("DateUTC", info.Date.UTC().String())
		}
	}
	if info.Database != "" {
		e.Str("Database", info.Database)
	}
	if dryRun {
		e.Msg("deleting dump (dry-run)")
		return nil
	}
	e.Msg("deleting dump")
	if err := d.st.DeleteAll(ctx, info.DumpID); err != nil {
		return err
	}
	return nil
}

func toStatus(hbs heartbeat.Status) DumpStatus {
	switch hbs {
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
