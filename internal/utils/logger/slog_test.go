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

package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withGlobalLogger(t *testing.T, level zerolog.Level) *bytes.Buffer {
	t.Helper()
	prev := log.Logger
	buf := &bytes.Buffer{}
	log.Logger = zerolog.New(buf).Level(level)
	t.Cleanup(func() { log.Logger = prev })
	return buf
}

func TestNewSlog_Enabled(t *testing.T) {
	ctx := context.Background()
	l := NewSlog("storages")

	withGlobalLogger(t, zerolog.InfoLevel)
	assert.False(t, l.Enabled(ctx, slog.LevelDebug))
	assert.True(t, l.Enabled(ctx, slog.LevelInfo))

	// The adapter must follow a logger replaced after it was created.
	withGlobalLogger(t, zerolog.DebugLevel)
	assert.True(t, l.Enabled(ctx, slog.LevelDebug))
}

func TestNewSlog_Handle(t *testing.T) {
	buf := withGlobalLogger(t, zerolog.DebugLevel)

	NewSlog("storages").With("backend", "s3", slog.Attr{}).WithGroup("req").
		With(slog.Group("", "inline", true), slog.Attr{}).
		Warn("slow request",
			"path", "a/b", "attempt", 2, "err", errors.New("boom"), slog.Group("http", "status", 503),
			slog.Group("", "rec_inline", 1), slog.Attr{})

	var got map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, map[string]any{
		"level":           "warn",
		"message":         "slow request",
		"source":          "storages",
		"backend":         "s3",
		"req.path":        "a/b",
		"req.attempt":     float64(2),
		"req.err":         "boom",
		"req.http.status": float64(503),
		"req.inline":      true,
		"req.rec_inline":  float64(1),
	}, got)
}
