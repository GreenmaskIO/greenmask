package delete_backup

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd/dump"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"log"
)

var (
	DeleteCmd = &cobra.Command{
		Use:  "delete",
		Args: cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := deleteDump(args[0]); err != nil {
				log.Fatal(err)
			}
		},
	}
)

func init() {
	log.SetPrefix("")
}

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
		id, err := b.Dirname(ctx)
		if err != nil {
			return err
		}
		if id == dumpId {
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
