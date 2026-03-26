package cmdrun

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/common/heartbeat"
	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
)

const (
	MetadataJsonFileName = "metadata.json"
)

type Filter struct {
	Tags     []string           `json:"tags"`
	Statuses []heartbeat.Status `json:"statuses"`
}

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

func NewFilter(tags []string, statuses []string) (*Filter, error) {
	var statusEnums []heartbeat.Status
	for _, statusStr := range statuses {
		status := heartbeat.Status(statusStr)
		if err := status.Validate(); err != nil {
			return nil, fmt.Errorf("validate status '%s': %w", statusStr, err)
		}
		statusEnums = append(statusEnums, heartbeat.Status(statusStr))
	}
	return &Filter{
		Tags:     tags,
		Statuses: statusEnums,
	}, nil
}

func (f *Filter) Match(metadata models.Metadata, status heartbeat.Status) bool {
	if len(f.Tags) == 0 && len(f.Statuses) == 0 {
		return true
	}
	if len(f.Tags) > 0 {
		for _, tag := range f.Tags {
			if !slices.Contains(metadata.Tags, tag) {
				return false
			}
		}
	}
	if len(f.Statuses) > 0 {
		if !slices.Contains(f.Statuses, status) {
			return false
		}
	}
	return true
}

// RunListDumps - list all dumps in the storage
func RunListDumps(cfg *config.Config, quiet bool, format OutputFormat, f *Filter) error {
	ctx := context.Background()
	ctx = log.Ctx(ctx).With().
		Str(models.MetaKeyEngine, string(cfg.Engine)).
		Logger().
		WithContext(ctx)

	st, err := utils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	if err := utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	if cfg.Engine == "" {
		return fmt.Errorf("specify dbms engine in \"engine\" key in the config: %w", errEngineNotSpecified)
	}

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		return err
	}

	if quiet {
		return printDumpIDsSorted(ctx, cfg, dirs, f)
	}
	if format == OutputFormatJSON {
		return printDumpJSON(ctx, cfg, dirs, f)
	}
	return printDumpTablePretty(ctx, cfg, dirs, f)
}

func readMetadata(ctx context.Context, st interfaces.Storager) (models.Metadata, error) {
	r, err := st.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return models.Metadata{}, fmt.Errorf("get metadata.json object: %w", err)
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close metadata.json object")
		}
	}()

	var res models.Metadata
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return models.Metadata{}, fmt.Errorf("decode metadata.json: %w", err)
	}
	return res, nil
}

func getMetadataAndStatus(ctx context.Context, cfg *config.Config, st interfaces.Storager) (heartbeat.Status, models.Metadata, error) {
	r := heartbeat.NewReader(st).SetStaleTimeout(cfg.Common.HeartbeatInterval)
	status, err := r.Read(ctx)
	if err != nil {
		return "", models.Metadata{}, fmt.Errorf("read dump status: %w", err)
	}
	metadata, err := readMetadata(ctx, st)
	if err != nil {
		if errors.Is(err, models.ErrFileNotFound) {
			if status == heartbeat.StatusDone {
				return heartbeat.StatusFailed, models.Metadata{}, nil
			}
			return status, models.Metadata{}, nil
		}
		return "", models.Metadata{}, fmt.Errorf("read dump metadata: %w", err)
	}
	return status, metadata, nil
}

func printDumpIDsSorted(ctx context.Context, cfg *config.Config, dirs []interfaces.Storager, f *Filter) error {
	dumpIDs := make([]string, 0, len(dirs))

	for _, backup := range dirs {
		status, metadata, err := getMetadataAndStatus(ctx, cfg, backup)
		if err != nil {
			return fmt.Errorf("get metadata and status : %w", err)
		}
		if !f.Match(metadata, status) {
			log.Ctx(ctx).Debug().
				Str("DumpId", backup.Dirname()).
				Msg("dump does not match the filter, skipping")
			continue
		}
		dumpIDs = append(dumpIDs, backup.Dirname())
	}
	slices.SortFunc(dumpIDs, func(a, b string) int {
		return cmp.Compare(b, a) // reverse order
	})
	for _, id := range dumpIDs {
		fmt.Println(id)
	}
	return nil
}

func getDumpInfo(ctx context.Context, cfg *config.Config, st interfaces.Storager, f *Filter) (*DumpListItem, bool, error) {
	dumpId := st.Dirname()
	status, metadata, err := getMetadataAndStatus(ctx, cfg, st)
	if err != nil {
		return nil, false, fmt.Errorf("get metadata and status : %w", err)
	}
	if !f.Match(metadata, status) {
		log.Ctx(ctx).Debug().
			Str("DumpId", dumpId).
			Msg("dump does not match the filter, skipping")
		return nil, false, nil
	}

	var creationDate, duration string
	var size, compressedSize int64
	var transformed bool
	if status == heartbeat.StatusDone {
		creationDate = metadata.CompletedAt.Format(time.RFC3339)
		size = metadata.OriginalSize
		compressedSize = metadata.CompressedSize
		diff := metadata.CompletedAt.Sub(metadata.StartedAt)
		duration = time.Time{}.Add(diff).Format("15:04:05")
		if metadata.DataDump != nil && len(metadata.DataDump.Transformers) > 0 {
			transformed = true
		}
	}

	return &DumpListItem{
		ID:             dumpId,
		Date:           creationDate,
		Engine:         string(metadata.Engine),
		Databases:      metadata.Databases,
		Size:           size,
		CompressedSize: compressedSize,
		Duration:       duration,
		Transformed:    transformed,
		Status:         status,
		Description:    metadata.Description,
		Tags:           metadata.Tags,
	}, true, nil
}

func printDumpTablePretty(ctx context.Context, cfg *config.Config, dirs []interfaces.Storager, f *Filter) error {
	var data [][]string
	for _, backup := range dirs {
		dumpId := backup.Dirname()
		info, ok, err := getDumpInfo(ctx, cfg, backup, f)
		if err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("Hint", "delete the dump if it's corrupted").
				Str("DumpId", dumpId).
				Msg("cannot get dump info, skipping")
			continue
		}
		if !ok {
			continue
		}

		description := info.Description
		if len(description) > 60 {
			description = description[:57] + "..."
		}

		data = append(data, []string{
			info.ID,
			info.Date,
			info.Engine,
			strings.Join(info.Databases, ", "),
			utils.SizePretty(info.Size),
			utils.SizePretty(info.CompressedSize),
			info.Duration,
			fmt.Sprintf("%t", info.Transformed),
			string(info.Status),
			description,
			strings.Join(info.Tags, ", "),
		})
	}
	slices.SortFunc(data, func(a, b []string) int {
		return cmp.Compare(b[0], a[0]) // reverse order by id
	})
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"id", "date", "engine",
		"databases", "size", "compressed size",
		"duration", "transformed", "status",
		"description", "tags",
	})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func printDumpJSON(ctx context.Context, cfg *config.Config, dirs []interfaces.Storager, f *Filter) error {
	var results []*DumpListItem
	for _, backup := range dirs {
		dumpId := backup.Dirname()
		info, ok, err := getDumpInfo(ctx, cfg, backup, f)
		if err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("Hint", "delete the dump if it's corrupted").
				Str("DumpId", dumpId).
				Msg("cannot get dump info, skipping")
			continue
		}
		if !ok {
			continue
		}
		results = append(results, info)
	}
	slices.SortFunc(results, func(a, b *DumpListItem) int {
		return cmp.Compare(b.ID, a.ID) // reverse order by id
	})
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(results)
}
