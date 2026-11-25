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
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
)

// Repro for GreenmaskIO/greenmask#575: scripts of a skipped section are
// dropped silently. Each skipped section that has scripts configured must
// produce a warning naming the section and the option that skipped it.
// These tests call the early-return paths only, so no database is needed.
func TestRestore_SkippedSectionScriptsWarn(t *testing.T) {
	scripts := map[string][]pgrestore.Script{
		scriptPreDataSection: {
			{Name: "prepare-table", When: scriptExecuteBefore, Query: "CREATE TABLE t (a int)"},
		},
		scriptDataSection: {
			{Name: "prepare-data", When: scriptExecuteBefore, Query: "SELECT 1"},
		},
		scriptPostDataSection: {
			{Name: "rebuild-index", When: scriptExecuteAfter, Query: "SELECT 1"},
		},
	}

	newRestore := func(opt *pgrestore.Options) *Restore {
		return &Restore{restoreOpt: opt, scripts: scripts}
	}

	logLines := func(t *testing.T, buf *syncBuffer) []map[string]any {
		t.Helper()
		var lines []map[string]any
		for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
			if line == "" {
				continue
			}
			var entry map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			lines = append(lines, entry)
		}
		return lines
	}

	assertWarnsSkippedScripts := func(t *testing.T, buf *syncBuffer, section, option string) {
		t.Helper()
		lines := logLines(t, buf)
		require.NotEmpty(t, lines, "expected a warning for skipped %q scripts, log is silent", section)
		found := false
		for _, entry := range lines {
			if entry["level"] != "warn" {
				continue
			}
			msg, _ := entry["message"].(string)
			if strings.Contains(msg, section) && strings.Contains(msg, option) {
				found = true
			}
		}
		assert.True(t, found, "expected a warn log naming section %q and option %q, got: %s", section, option, buf.String())
	}

	t.Run("pre-data scripts skipped by --data-only", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{DataOnly: true})
		require.NoError(t, r.preDataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "pre-data", "--data-only")
	})

	t.Run("post-data scripts skipped by --data-only", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{DataOnly: true})
		require.NoError(t, r.postDataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "post-data", "--data-only")
	})

	t.Run("data scripts skipped by --schema-only", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{SchemaOnly: true})
		require.NoError(t, r.dataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "data", "--schema-only")
	})

	t.Run("pre-data scripts skipped by --section=data", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{Section: "data"})
		require.NoError(t, r.preDataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "pre-data", "--section=data")
	})

	t.Run("data scripts skipped by --section=pre-data", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{Section: "pre-data"})
		require.NoError(t, r.dataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "data", "--section=pre-data")
	})

	t.Run("post-data scripts skipped by --section=data", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{Section: "data"})
		require.NoError(t, r.postDataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "post-data", "--section=data")
	})

	t.Run("pre-data scripts skipped by --data-only and --section name both", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := newRestore(&pgrestore.Options{DataOnly: true, Section: "post-data"})
		require.NoError(t, r.preDataRestore(context.Background()))
		assertWarnsSkippedScripts(t, buf, "pre-data", "--data-only")
		assert.Contains(t, buf.String(), "--section=post-data")
	})

	t.Run("no warning when section has no scripts", func(t *testing.T) {
		buf := captureLog(t, zerolog.DebugLevel)
		r := &Restore{
			restoreOpt: &pgrestore.Options{DataOnly: true},
			scripts:    map[string][]pgrestore.Script{},
		}
		require.NoError(t, r.preDataRestore(context.Background()))
		assert.Empty(t, buf.String(), "no scripts configured, no warning expected")
	})
}
