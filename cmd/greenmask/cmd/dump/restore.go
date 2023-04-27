package dump

import (
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	ListDumpCmd = &cobra.Command{
		Use: "list-dump",
		Run: func(cmd *cobra.Command, args []string) {
			log.Fatal().Msg("does not implemented")
		},
	}
)

func init() {
	ListDumpCmd.Flags().StringVar(&cfgFile, "test", "", "test flag")
}
