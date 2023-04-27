package main

import (
	"github.com/rs/zerolog/log"
	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd"
	"time"
)

func main() {
	startTime := time.Now()
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("Fatal")
	}
	log.Debug().Msgf("uptime %f", time.Since(startTime).Seconds())
}
