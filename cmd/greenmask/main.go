package main

import (
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd"
)

var version string

func main() {
	cmd.RootCmd.Version = version
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
