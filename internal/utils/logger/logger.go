package logger

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	LogFormatJsonValue = "json"
	LogFormatTextValue = "text"
)

func SetLogLevel(logLevelStr string, logFormat string) error {

	var logLevel zerolog.Level
	switch logLevelStr {
	case zerolog.LevelDebugValue:
		logLevel = zerolog.DebugLevel
	case zerolog.LevelInfoValue:
		logLevel = zerolog.InfoLevel
	case zerolog.LevelWarnValue:
		logLevel = zerolog.WarnLevel
	default:
		return fmt.Errorf("unknown log level %s", logLevelStr)

	}

	var formatWriter io.Writer
	switch logFormat {
	case LogFormatJsonValue:
		formatWriter = os.Stderr
	case LogFormatTextValue:
		formatWriter = zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}
	}

	if logLevelStr == zerolog.LevelDebugValue {
		log.Logger = zerolog.New(formatWriter).
			Level(logLevel).
			With().
			Timestamp().
			Caller().
			Int("pid", os.Getpid()).Logger()
	} else {
		log.Logger = zerolog.New(formatWriter).
			Level(logLevel).
			With().
			Timestamp().
			Logger()
	}
	return nil
}
