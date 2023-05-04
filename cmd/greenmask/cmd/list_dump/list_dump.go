package list_dump

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
)

var (
	ListDumpCmd = &cobra.Command{
		Use: "list-dump",
		Run: func(cmd *cobra.Command, args []string) {
			if err := listDumps(); err != nil {
				log.Fatal(err)
			}
		},
	}
)

func init() {
	log.SetPrefix("")
}

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
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal(err)
	}

	var data [][]string

	for _, backup := range dirs {
		dumpId, err := backup.Dirname(ctx)
		if err != nil {
			log.Fatal(err)
		}
		backupFiles, _, err := backup.ListDir(ctx)
		if err != nil {
			log.Fatalf("cannot walk through backup: %s", err.Error())
		}
		var metadataFound bool
		var status = "done"
		for _, fileName := range backupFiles {
			if fileName == "metadata.json" {
				metadataFound = true
				break
			}
		}
		if !metadataFound {
			status = "unknown or failed"
		}

		var creationDate, dbName, size, compressedSize, duration, transformed string
		transformed = "false"
		if metadataFound {
			metadata, err := getMetadata(ctx, backup)
			if err != nil {
				log.Fatal(err)
			}
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
		log.Fatal(err)
	}
	defer mf.Close()

	metadata := &pgDomains.Metadata{}
	if err = json.NewDecoder(mf).Decode(metadata); err != nil {
		return nil, fmt.Errorf("unable to read metadata: %w", err)
	}
	return metadata, nil
}
