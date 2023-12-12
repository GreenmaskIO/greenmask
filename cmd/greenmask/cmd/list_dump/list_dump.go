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

package list_dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	pgDomains "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
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
				log.Err(err).Msg("")
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

		var status = "done"
		metadataFound, err := backup.Exists(ctx, "metadata.json")
		if err != nil {
			log.Err(err).Msg("")
		}
		if !metadataFound {
			status = "unknown or failed"
		}

		var creationDate, dbName, size, compressedSize, duration, transformed string
		transformed = "false"
		if metadataFound {
			metadata, err := getMetadata(ctx, backup)
			if err != nil {
				log.Debug().Err(err).Msg("")
			}
			if err == nil {
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
		}

		data = append(data, []string{
			dumpId,
			creationDate,
			dbName,
			size,
			compressedSize,
			duration,
			transformed,
			status,
		})
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

func getMetadata(ctx context.Context, st storages.Storager) (*pgDomains.Metadata, error) {
	mf, err := st.GetObject(ctx, "metadata.json")
	if err != nil {
		log.Err(err).Msg("")
	}
	defer mf.Close()

	metadata := &pgDomains.Metadata{}
	if err = json.NewDecoder(mf).Decode(metadata); err != nil {
		return nil, fmt.Errorf("unable to read metadata: %w", err)
	}
	return metadata, nil
}
