package main

import "github.com/rs/zerolog/log"

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
