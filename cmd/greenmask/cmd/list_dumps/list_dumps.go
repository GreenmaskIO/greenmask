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

package list_dumps

import (
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/dumpstatus"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "list-dumps",
		Short: "list all dumps in the storage",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Err(err).Msg("")
			}

			if err := listDumps(); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
	Config = domains.NewConfig()
)

func SizePretty(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

func listDumps() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		return err
	}

	var data [][]string

	for _, backup := range dirs {
		dumpId := backup.Dirname()
		if err = renderListItem(ctx, backup, &data); err != nil {
			log.Warn().
				Err(err).
				Str("DumpId", dumpId).
				Msg("unable to render list dump item")
		}
	}

	slices.SortFunc(data, func(a, b []string) int {
		if a[0] > b[0] {
			return -1
		}
		return 1
	})

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"id", "date", "database", "size", "compressed size", "duration", "transformed", "status"})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func renderListItem(ctx context.Context, st storages.Storager, data *[][]string) error {
	dumpId := st.Dirname()

	status, metadata, err := dumpstatus.GetDumpStatusAndMetadata(ctx, st)
	if err != nil {
		return fmt.Errorf("failed to get status and metadata: %w", err)
	}

	var creationDate, dbName, size, compressedSize, duration, transformed string
	transformed = "false"
	if status == dumpstatus.DoneStatusName {
		creationDate = metadata.Header.CreationDate.Format(time.RFC3339)
		dbName = metadata.Header.DbName
		size = SizePretty(metadata.OriginalSize)
		compressedSize = SizePretty(metadata.CompressedSize)
		diff := metadata.CompletedAt.Sub(metadata.StartedAt)
		duration = time.Time{}.Add(diff).Format("15:04:05")
		if len(metadata.Transformers) > 0 {
			transformed = "true"
		}
	}

	*data = append(*data, []string{
		dumpId,
		creationDate,
		dbName,
		size,
		compressedSize,
		duration,
		transformed,
		status,
	})
	return nil
}
