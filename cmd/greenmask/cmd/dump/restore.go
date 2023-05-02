package dump

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	RestoreCmd = &cobra.Command{
		Use: "restore",
		Run: func(cmd *cobra.Command, args []string) {
			log.Fatal().Msg("does not implemented")
		},
	}
)

func init() {
}
