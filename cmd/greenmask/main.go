package main

import (
	"log"

	"github.com/wwoytenko/greenfuscator/cmd/greenmask/cmd"
)

func main() {
	log.SetFlags(0)
	if err := cmd.Execute(); err != nil {
		log.Fatalln(err)
	}
}
