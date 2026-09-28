// Copyright 2023 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncBuffer is an io.Writer safe for concurrent use. The notice handler runs on
// the goroutine using its connection, so with several connections it runs concurrently.
type syncBuffer struct {
	mx  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mx.Lock()
	defer b.mx.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mx.Lock()
	defer b.mx.Unlock()
	return b.buf.String()
}

// captureLog swaps the global logger for one writing JSON into a buffer.
//
// The global logger is process-wide state, so tests using this cannot call
// t.Parallel: they would swap it out from under each other. zerolog's global
// logger is how the rest of the codebase logs, so capturing it is the price of
// testing the handler as it actually runs.
func captureLog(t *testing.T, level zerolog.Level) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	original := log.Logger
	originalLevel := zerolog.GlobalLevel()
	log.Logger = zerolog.New(buf)
	zerolog.SetGlobalLevel(level)
	t.Cleanup(func() {
		log.Logger = original
		zerolog.SetGlobalLevel(originalLevel)
	})
	return buf
}

func decodeLine(t *testing.T, buf *syncBuffer) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(buf.String()), &out))
	return out
}

// levelsByMessage decodes every log line and maps its message to its level.
func levelsByMessage(t *testing.T, buf *syncBuffer) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		msg, _ := entry["message"].(string)
		level, _ := entry["level"].(string)
		out[msg] = level
	}
	return out
}

// scriptNotices returns the handler openConn attaches, bound to the current global logger.
func scriptNotices() pgconn.NoticeHandler {
	return newNoticeHandler(log.Logger, scriptNoticeLevels)
}

func TestNoticeHandler(t *testing.T) {
	t.Run("NOTICE is logged at info", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			SeverityUnlocalized: "NOTICE",
			Message:             "Re-applied grants: schema=fault",
		})

		entry := decodeLine(t, buf)
		assert.Equal(t, "info", entry["level"])
		assert.Equal(t, "Re-applied grants: schema=fault", entry["message"])
		assert.Equal(t, "NOTICE", entry["severity"])
	})

	t.Run("WARNING is logged at warn", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			SeverityUnlocalized: "WARNING",
			Message:             "nothing matched",
		})

		assert.Equal(t, "warn", decodeLine(t, buf)["level"])
	})

	t.Run("DEBUG is logged at debug", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			SeverityUnlocalized: "DEBUG",
			Message:             "internal detail",
		})

		assert.Equal(t, "debug", decodeLine(t, buf)["level"])
	})

	t.Run("INFO, LOG and unknown severities are logged at info", func(t *testing.T) {
		for _, severity := range []string{"INFO", "LOG", "SOMETHING_NEW"} {
			buf := captureLog(t, zerolog.DebugLevel)
			scriptNotices()(nil, &pgconn.Notice{
				SeverityUnlocalized: severity, Message: "informational",
			})

			entry := decodeLine(t, buf)
			assert.Equal(t, "info", entry["level"], "severity %s", severity)
			assert.Equal(t, severity, entry["severity"])
		}
	})

	t.Run("detail and hint are attached when present", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			SeverityUnlocalized: "NOTICE",
			Message:             "message",
			Detail:              "some detail",
			Hint:                "some hint",
		})

		entry := decodeLine(t, buf)
		assert.Equal(t, "some detail", entry["detail"])
		assert.Equal(t, "some hint", entry["hint"])
	})

	t.Run("detail and hint are omitted when empty", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			SeverityUnlocalized: "NOTICE", Message: "message",
		})

		entry := decodeLine(t, buf)
		assert.NotContains(t, entry, "detail")
		assert.NotContains(t, entry, "hint")
	})

	t.Run("falls back to the localised severity", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, &pgconn.Notice{
			Severity: "HINWEIS", Message: "localised server",
		})

		entry := decodeLine(t, buf)
		assert.Equal(t, "info", entry["level"])
		assert.Equal(t, "HINWEIS", entry["severity"])
	})

	t.Run("a nil notice is ignored", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		scriptNotices()(nil, nil)
		assert.Empty(t, buf.String())
	})

	t.Run("notices respect the configured log level", func(t *testing.T) {
		buf := captureLog(t, zerolog.WarnLevel)
		h := scriptNotices()
		h(nil, &pgconn.Notice{SeverityUnlocalized: "NOTICE", Message: "suppressed"})
		assert.Empty(t, buf.String(), "NOTICE must not appear when level is warn")

		h(nil, &pgconn.Notice{SeverityUnlocalized: "WARNING", Message: "shown"})
		assert.Contains(t, buf.String(), "shown")
	})
}

func TestWorkerNoticeHandler(t *testing.T) {
	workerNotices := func() pgconn.NoticeHandler {
		return newNoticeHandler(log.With().Int(workerIDLogKey, 3).Logger(), workerNoticeLevels)
	}

	t.Run("informational notices are demoted to debug", func(t *testing.T) {
		for _, severity := range []string{"NOTICE", "INFO", "LOG"} {
			buf := captureLog(t, zerolog.DebugLevel)
			workerNotices()(nil, &pgconn.Notice{SeverityUnlocalized: severity, Message: "per row"})

			assert.Equal(t, "debug", decodeLine(t, buf)["level"], "severity %s", severity)
		}
	})

	t.Run("WARNING stays at warn", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		workerNotices()(nil, &pgconn.Notice{SeverityUnlocalized: "WARNING", Message: "bad row"})

		assert.Equal(t, "warn", decodeLine(t, buf)["level"])
	})

	t.Run("notices carry the worker id", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		workerNotices()(nil, &pgconn.Notice{SeverityUnlocalized: "WARNING", Message: "bad row"})

		assert.EqualValues(t, 3, decodeLine(t, buf)[workerIDLogKey])
	})

	t.Run("demoted notices are hidden at info", func(t *testing.T) {
		buf := captureLog(t, zerolog.InfoLevel)
		workerNotices()(nil, &pgconn.Notice{SeverityUnlocalized: "NOTICE", Message: "per row"})

		assert.Empty(t, buf.String())
	})
}

// TestNoticeHandlerConcurrent calls the handler from several goroutines, as
// happens with one connection per worker. Run with -race.
func TestNoticeHandlerConcurrent(t *testing.T) {
	const (
		goroutines = 8
		perRoutine = 50
	)
	buf := captureLog(t, zerolog.DebugLevel)
	h := scriptNotices()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perRoutine; i++ {
				h(nil, &pgconn.Notice{
					SeverityUnlocalized: "NOTICE",
					Message:             fmt.Sprintf("notice g%d-%d", g, i),
				})
			}
		}(g)
	}
	wg.Wait()

	logged := buf.String()
	assert.Equal(t, goroutines*perRoutine, strings.Count(logged, `"severity":"NOTICE"`),
		"every notice must be logged exactly once")
	for g := 0; g < goroutines; g++ {
		assert.Contains(t, logged, fmt.Sprintf("notice g%d-0", g))
	}
}
