package main

import (
	"encoding/json"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
)

func main() {
	res := make(toolkit.RawRecordDto)
	res["test"] = &toolkit.RawValueDto{
		Data:   "1234",
		IsNull: false,
	}
	data, err := json.Marshal(res)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	eres := make(toolkit.RawRecordDto)
	err = json.Unmarshal(data, &eres)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	println(eres)
}
