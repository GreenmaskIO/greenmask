package main

import (
	"github.com/rs/zerolog/log"
	"os"
)

func main() {
	f, err := os.Open("/home/vadim/gits/woyten/greenfuscator/cmd/test/test.txt")
	if err != nil {
		log.Fatal().Err(err).Msg("error")
	}
	defer f.Close()

	buf := make([]byte, 32)
	for {
		n, err := f.Read(buf)
		if err != nil {
			log.Debug().Err(err).Msg("error")
			break
		}
		log.Printf("str = %s", buf[:n])
	}

}
