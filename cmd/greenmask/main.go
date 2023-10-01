package main

import (
	"github.com/rs/zerolog/log"
	"net/http"

	_ "net/http/pprof"

	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd"
)

func main() {
	go func() {
		http.ListenAndServe("localhost:8080", nil)
	}()

	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
