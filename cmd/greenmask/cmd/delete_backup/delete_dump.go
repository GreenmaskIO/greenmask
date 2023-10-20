package delete_backup

import (
	"context"
	"fmt"
	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/rs/zerolog/log"

	"github.com/spf13/cobra"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("")
			}

			if err := deleteDump(args[0]); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
	Config = pgDomains.NewConfig()
)

func deleteDump(dumpId string) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("")
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
	if err = st.Delete(ctx, dumpId); err != nil {
		return fmt.Errorf("storage error: %s", err)
	}

	return nil
}
