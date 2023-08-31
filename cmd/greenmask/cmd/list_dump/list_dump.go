package list_dump

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/config"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/storage"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"github.com/wwoytenko/greenfuscator/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use: "list-dump",
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Common.LogLevel, Config.Common.LogFormat); err != nil {
				log.Err(err).Msg("")
			}

			if err := listDumps(); err != nil {
				log.Err(err).Msg("")
			}
		},
	}
	Config = config.NewConfig()
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
	st, err := directory.NewDirectory(dump.Config.Common.Storage.Directory.Path, 0750, 0650)
	if err != nil {
		log.Err(err).Msg("")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Err(err).Msg("")
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

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"id", "date", "database", "size", "compressed size", "duration", "transformed", "status"})
	table.AppendBulk(data)
	table.Render()
	return nil
}

func getMetadata(ctx context.Context, st storage.Storager) (*pgDomains.Metadata, error) {
	mf, err := st.GetReader(ctx, "metadata.json")
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
