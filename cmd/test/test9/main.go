package main

import (
	"encoding/json"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/rs/zerolog/log"
)

func main() {
	res := make(transformers.RawRecordDto)
	res["test"] = &transformers.RawValueDto{
		Data:   "1234",
		IsNull: false,
	}
	data, err := json.Marshal(res)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	eres := make(transformers.RawRecordDto)
	err = json.Unmarshal(data, &eres)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
	println(eres)
}
