package cmdrun

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/heartbeat"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	MetadataJsonFileName = "metadata.json"
)

type Filter struct {
	Tags     []string           `json:"tags"`
	Statuses []heartbeat.Status `json:"statuses"`
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

func (f *Filter) Match(metadata commonmodels.Metadata, status heartbeat.Status) bool {
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
func RunListDumps(cfg *config.Config, quiet bool, f *Filter) error {
	ctx := context.Background()
	ctx = log.Ctx(ctx).With().
		Str(commonmodels.MetaKeyEngine, cfg.Engine).
		Logger().
		WithContext(ctx)

	st, err := commonutils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	if err := commonutils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
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
		return printDumpIDsSorted(ctx, st, dirs, f)
	}
	return printDumpTablePretty(ctx, dirs, f)
}

func readMetadata(ctx context.Context, st storages.Storager) (commonmodels.Metadata, error) {
	r, err := st.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("get metadata.json object: %w", err)
	}
	defer r.Close()

	var res commonmodels.Metadata
	if err := json.NewDecoder(r).Decode(&res); err != nil {
		return commonmodels.Metadata{}, fmt.Errorf("decode metadata.json: %w", err)
	}
	return res, nil
}

func getMetadataAndStatus(ctx context.Context, st storages.Storager) (heartbeat.Status, commonmodels.Metadata, error) {
	r := heartbeat.NewReader(st)
	status, err := r.Read(ctx)
	if err != nil {
		return "", commonmodels.Metadata{}, fmt.Errorf("read dump status: %w", err)
	}
	metadata, err := readMetadata(ctx, st)
	if err != nil {
		return "", commonmodels.Metadata{}, fmt.Errorf("read dump metadata: %w", err)
	}
	return status, metadata, nil
}

func printDumpIDsSorted(ctx context.Context, st storages.Storager, dirs []storages.Storager, f *Filter) error {
	dumpIDs := make([]string, 0, len(dirs))

	for _, backup := range dirs {
		status, metadata, err := getMetadataAndStatus(ctx, st)
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

func renderListItem(ctx context.Context, st storages.Storager, f *Filter, data *[][]string) error {
	dumpId := st.Dirname()
	status, metadata, err := getMetadataAndStatus(ctx, st)
	if err != nil {
		return fmt.Errorf("get metadata and status : %w", err)
	}
	if !f.Match(metadata, status) {
		log.Ctx(ctx).Debug().
			Str("DumpId", dumpId).
			Msg("dump does not match the filter, skipping")
		return nil
	}

	var creationDate, dbName, size, compressedSize, duration, transformed string
	transformed = "false"
	if status == heartbeat.StatusDone {
		creationDate = metadata.CompletedAt.Format(time.RFC3339)
		dbName = metadata.DatabaseName
		size = commonutils.SizePretty(metadata.OriginalSize)
		compressedSize = commonutils.SizePretty(metadata.CompressedSize)
		diff := metadata.CompletedAt.Sub(metadata.StartedAt)
		duration = time.Time{}.Add(diff).Format("15:04:05")
		if len(metadata.Transformers) > 0 {
			transformed = "true"
		}
	}

	description := metadata.Description
	if len(description) > 60 {
		description = description[:57] + "..."
	}

	*data = append(*data, []string{
		dumpId,
		creationDate,
		metadata.Engine,
		dbName,
		size,
		compressedSize,
		duration,
		transformed,
		string(status),
		description,
		strings.Join(metadata.Tags, ", "),
	})
	return nil
}

func printDumpTablePretty(ctx context.Context, dirs []storages.Storager, f *Filter) error {
	var data [][]string
	for _, backup := range dirs {
		dumpId := backup.Dirname()
		if err := renderListItem(ctx, backup, f, &data); err != nil {
			log.Ctx(ctx).
				Warn().
				Err(err).
				Str("Hint", "delete the dump if it's corrupted").
				Str("DumpId", dumpId).
				Msg("cannot render dump info, skipping")
		}
	}
	slices.SortFunc(data, func(a, b []string) int {
		return cmp.Compare(b[0], a[0]) // reverse order by id
	})
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{
		"id", "date", "engine",
		"database", "size", "compressed size",
		"duration", "transformed", "status",
		"description", "tags",
	})
	table.AppendBulk(data)
	table.Render()
	return nil
}
