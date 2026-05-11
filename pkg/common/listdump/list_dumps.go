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

// Package listdump provides engine-agnostic dump listing and filtering.
// It reads heartbeat and metadata from storage and exposes structured
// DumpListItem values suitable for programmatic consumption (gm-backend,
// CLI, API). All formatting/printing belongs to the caller.
package listdump

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
)

const MetadataFileName = "metadata.json"

// Filter selects dumps by tag and/or status. An empty Filter matches all dumps.
type Filter struct {
	Tags     []string           `json:"tags"`
	Statuses []heartbeat.Status `json:"statuses"`
}

// NewFilter validates and constructs a Filter. Returns an error when any
// status string is not a recognised heartbeat.Status value.
func NewFilter(tags []string, statuses []string) (*Filter, error) {
	statusEnums := make([]heartbeat.Status, 0, len(statuses))
	for _, s := range statuses {
		st := heartbeat.Status(s)
		if err := st.Validate(); err != nil {
			return nil, fmt.Errorf("validate status %q: %w", s, err)
		}
		statusEnums = append(statusEnums, st)
	}
	return &Filter{Tags: tags, Statuses: statusEnums}, nil
}

// Match reports whether a dump with the given metadata and heartbeat status
// satisfies the filter. All specified tags must be present; status must be
// in the allowed set (if any are set).
func (f *Filter) Match(metadata models.Metadata, status heartbeat.Status) bool {
	if len(f.Tags) == 0 && len(f.Statuses) == 0 {
		return true
	}
	for _, tag := range f.Tags {
		if !slices.Contains(metadata.Tags, tag) {
			return false
		}
	}
	if len(f.Statuses) > 0 && !slices.Contains(f.Statuses, status) {
		return false
	}
	return true
}

// DumpListItem summarises one dump entry for display or programmatic use.
// Date and Duration are pre-formatted strings (RFC3339 and HH:MM:SS) so
// consumers do not need to reformat them.
type DumpListItem struct {
	ID             string           `json:"id"`
	Date           string           `json:"date"`
	Engine         string           `json:"engine"`
	Databases      []string         `json:"databases"`
	Size           int64            `json:"size"`
	CompressedSize int64            `json:"compressed_size"`
	Duration       string           `json:"duration"`
	Transformed    bool             `json:"transformed"`
	Status         heartbeat.Status `json:"status"`
	Description    string           `json:"description"`
	Tags           []string         `json:"tags"`
}

// DatabaseList joins databases with a ", " separator for tabular display.
func (d *DumpListItem) DatabaseList() string {
	return strings.Join(d.Databases, ", ")
}

// TagList joins tags with a ", " separator for tabular display.
func (d *DumpListItem) TagList() string {
	return strings.Join(d.Tags, ", ")
}

// Lister retrieves dump metadata from a storage root.
type Lister struct {
	st                interfaces.Storager
	heartbeatInterval time.Duration
}

// New returns a Lister backed by st. heartbeatInterval is the stale-timeout
// forwarded to the heartbeat reader when determining dump status.
func New(st interfaces.Storager, heartbeatInterval time.Duration) *Lister {
	return &Lister{st: st, heartbeatInterval: heartbeatInterval}
}

// List returns all dumps that match f, sorted newest-first by dump ID.
// Dumps whose metadata or heartbeat cannot be read are logged as warnings
// and skipped (not treated as errors).
func (l *Lister) List(ctx context.Context, f *Filter) ([]DumpListItem, error) {
	_, dirs, err := l.st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("list storage directory: %w", err)
	}
	var results []DumpListItem
	for _, backup := range dirs {
		item, ok, err := l.readDumpItem(ctx, backup, f)
		if err != nil {
			log.Ctx(ctx).Warn().
				Err(err).
				Str("Hint", "delete the dump if it is corrupted").
				Str("DumpID", backup.Dirname()).
				Msg("cannot read dump info, skipping")
			continue
		}
		if ok {
			results = append(results, item)
		}
	}
	slices.SortFunc(results, func(a, b DumpListItem) int {
		return cmp.Compare(b.ID, a.ID)
	})
	return results, nil
}

// IDs returns the dump IDs of all dumps that match f, sorted newest-first.
// Dumps that cannot be read are logged as warnings and skipped.
func (l *Lister) IDs(ctx context.Context, f *Filter) ([]string, error) {
	_, dirs, err := l.st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("list storage directory: %w", err)
	}
	var ids []string
	for _, backup := range dirs {
		status, metadata, err := l.metadataAndStatus(ctx, backup)
		if err != nil {
			log.Ctx(ctx).Warn().
				Err(err).
				Str("DumpID", backup.Dirname()).
				Msg("cannot read dump status, skipping")
			continue
		}
		if !f.Match(metadata, status) {
			log.Ctx(ctx).Debug().
				Str("DumpID", backup.Dirname()).
				Msg("dump does not match the filter, skipping")
			continue
		}
		ids = append(ids, backup.Dirname())
	}
	slices.SortFunc(ids, func(a, b string) int {
		return cmp.Compare(b, a)
	})
	return ids, nil
}

// readDumpItem reads heartbeat + metadata for a single dump directory,
// applies the filter, and constructs a DumpListItem.
// ok=false means the dump was filtered out (not an error).
func (l *Lister) readDumpItem(ctx context.Context, backup interfaces.Storager, f *Filter) (DumpListItem, bool, error) {
	status, metadata, err := l.metadataAndStatus(ctx, backup)
	if err != nil {
		return DumpListItem{}, false, fmt.Errorf("get metadata and status: %w", err)
	}
	if !f.Match(metadata, status) {
		log.Ctx(ctx).Debug().
			Str("DumpID", backup.Dirname()).
			Msg("dump does not match the filter, skipping")
		return DumpListItem{}, false, nil
	}
	return buildItem(backup.Dirname(), status, metadata), true, nil
}

// metadataAndStatus reads the heartbeat status and metadata.json for a dump.
// When the heartbeat says "done" but metadata.json is absent, the status is
// downgraded to "failed" (consistent with list-dumps behaviour).
func (l *Lister) metadataAndStatus(ctx context.Context, st interfaces.Storager) (heartbeat.Status, models.Metadata, error) {
	status, err := heartbeat.NewReader(st).SetStaleTimeout(l.heartbeatInterval).Read(ctx)
	if err != nil {
		return "", models.Metadata{}, fmt.Errorf("read heartbeat: %w", err)
	}
	metadata, err := readMetadata(ctx, st)
	if err != nil {
		if errors.Is(err, models.ErrFileNotFound) {
			if status == heartbeat.StatusDone {
				return heartbeat.StatusFailed, models.Metadata{}, nil
			}
			return status, models.Metadata{}, nil
		}
		return "", models.Metadata{}, fmt.Errorf("read metadata: %w", err)
	}
	return status, metadata, nil
}

func readMetadata(ctx context.Context, st interfaces.Storager) (models.Metadata, error) {
	r, err := st.GetObject(ctx, MetadataFileName)
	if err != nil {
		return models.Metadata{}, err
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close metadata.json")
		}
	}()
	var md models.Metadata
	if err := json.NewDecoder(r).Decode(&md); err != nil {
		return models.Metadata{}, fmt.Errorf("decode metadata.json: %w", err)
	}
	return md, nil
}

func buildItem(dumpID string, status heartbeat.Status, md models.Metadata) DumpListItem {
	var date, duration string
	var size, compressedSize int64
	var transformed bool
	if status == heartbeat.StatusDone {
		date = md.CompletedAt.Format(time.RFC3339)
		size = md.OriginalSize
		compressedSize = md.CompressedSize
		diff := md.CompletedAt.Sub(md.StartedAt)
		duration = time.Time{}.Add(diff).Format("15:04:05")
		if md.DataDump != nil && len(md.DataDump.Transformers) > 0 {
			transformed = true
		}
	}
	return DumpListItem{
		ID:             dumpID,
		Date:           date,
		Engine:         string(md.Engine),
		Databases:      md.Databases,
		Size:           size,
		CompressedSize: compressedSize,
		Duration:       duration,
		Transformed:    transformed,
		Status:         status,
		Description:    md.Description,
		Tags:           md.Tags,
	}
}
