package delete_backup

import (
	"context"
	"fmt"
	"log"

	"github.com/spf13/cobra"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
	pgDomains "github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"github.com/wwoytenko/greenfuscator/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Common.LogLevel, Config.Common.LogFormat); err != nil {
				log.Fatal(err)
			}

			if err := deleteDump(args[0]); err != nil {
				log.Fatal(err)
			}
		},
	}
	Config = pgDomains.NewConfig()
)

func deleteDump(dumpId string) error {
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

	var found bool
	for _, b := range dirs {
		if dumpId == b.Dirname() {
			found = true
		}
	}

	if !found {
		return fmt.Errorf("dump with id %s was not found", dumpId)
	}
	if err = st.Delete(ctx, dumpId, true); err != nil {
		return fmt.Errorf("unable to deleteDump dump: %s", err)
	}

	return nil
}
