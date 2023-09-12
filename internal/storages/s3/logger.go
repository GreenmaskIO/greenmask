package s3

import (
	"fmt"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type LogWrapper struct {
	logger *zerolog.Logger
}

func (lw LogWrapper) Log(objs ...interface{}) {
	event := log.Debug()
	for idx, o := range objs {
		event.Any(fmt.Sprintf("%d", idx), o)
	}
	event.Msg("s3 storage logging")
}
