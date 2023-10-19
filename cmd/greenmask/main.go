package main

import (
	"github.com/rs/zerolog/log"

	//_ "net/http/pprof"

	"github.com/greenmaskio/greenmask/cmd/greenmask/cmd"
)

func main() {
	//go func() {
	//	http.ListenAndServe("localhost:8080", nil)
	//}()
	//time.Sleep(2 * time.Second)

	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
	//time.Sleep(20 * time.Second)
}
