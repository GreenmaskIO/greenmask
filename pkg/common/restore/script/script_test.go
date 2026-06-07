// Copyright 2025 Greenmask
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

package script

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Scheduler.Exec ---

func TestScheduler_Exec(t *testing.T) {
	ctx := context.Background()
	noopExec := TxExec(func(_ context.Context, _ string) error { return nil })

	tests := []struct {
		name           string
		scripts        []core.Script
		section        core.DumpSection
		when           core.ScriptEventType
		exec           TxExec
		wantErr        bool
		wantErrContain string
		wantCalled     int
	}{
		{
			name:       "no scripts — no error",
			scripts:    nil,
			section:    core.DumpSectionPreData,
			when:       core.ScriptEventTypeBefore,
			exec:       noopExec,
			wantCalled: 0,
		},
		{
			name: "script with non-matching section is skipped",
			scripts: []core.Script{
				{Name: "s", Section: core.DumpSectionPostData, When: core.ScriptEventTypeBefore, Query: "SELECT 1"},
			},
			section:    core.DumpSectionPreData,
			when:       core.ScriptEventTypeBefore,
			exec:       noopExec,
			wantCalled: 0,
		},
		{
			name: "script with non-matching when is skipped",
			scripts: []core.Script{
				{Name: "s", Section: core.DumpSectionPreData, When: core.ScriptEventTypeAfter, Query: "SELECT 1"},
			},
			section:    core.DumpSectionPreData,
			when:       core.ScriptEventTypeBefore,
			exec:       noopExec,
			wantCalled: 0,
		},
		{
			name: "matching script is executed",
			scripts: []core.Script{
				{Name: "s", Section: core.DumpSectionPreData, When: core.ScriptEventTypeBefore, Query: "SELECT 1"},
			},
			section:    core.DumpSectionPreData,
			when:       core.ScriptEventTypeBefore,
			exec:       noopExec,
			wantCalled: 1,
		},
		{
			name: "multiple matching scripts all executed",
			scripts: []core.Script{
				{Name: "a", Section: core.DumpSectionData, When: core.ScriptEventTypeAfter, Query: "SELECT 1"},
				{Name: "b", Section: core.DumpSectionData, When: core.ScriptEventTypeAfter, Query: "SELECT 2"},
			},
			section:    core.DumpSectionData,
			when:       core.ScriptEventTypeAfter,
			exec:       noopExec,
			wantCalled: 2,
		},
		{
			name: "executor error is propagated with script index",
			scripts: []core.Script{
				{Name: "skip", Section: core.DumpSectionPreData, When: core.ScriptEventTypeBefore, Query: "SELECT 1"},
				{Name: "fail", Section: core.DumpSectionData, When: core.ScriptEventTypeBefore, Query: "SELECT 1"},
			},
			section: core.DumpSectionData,
			when:    core.ScriptEventTypeBefore,
			exec: func(_ context.Context, _ string) error {
				return errors.New("boom")
			},
			wantErr:        true,
			wantErrContain: "execute script #1",
		},
		{
			name: "mixed: only matching scripts executed, non-matching skipped",
			scripts: []core.Script{
				{Name: "skip", Section: core.DumpSectionPostData, When: core.ScriptEventTypeBefore, Query: "SELECT 1"},
				{Name: "run", Section: core.DumpSectionPreData, When: core.ScriptEventTypeBefore, Query: "SELECT 2"},
			},
			section:    core.DumpSectionPreData,
			when:       core.ScriptEventTypeBefore,
			exec:       noopExec,
			wantCalled: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := 0
			var execFn TxExec
			if tt.wantErr {
				execFn = tt.exec
			} else {
				execFn = func(ctx context.Context, query string) error {
					called++
					return tt.exec(ctx, query)
				}
			}

			s := NewScheduler(tt.scripts)
			err := s.Exec(ctx, execFn, tt.section, tt.when)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantCalled, called)
			}
		})
	}
}

// --- Executor.Validate ---

func TestExecutor_Validate(t *testing.T) {
	tests := []struct {
		name           string
		script         core.Script
		wantErr        bool
		wantErrContain string
	}{
		{
			name: "valid with query",
			script: core.Script{
				Name:  "s",
				When:  core.ScriptEventTypeBefore,
				Query: "SELECT 1",
			},
		},
		{
			name: "valid with query_file",
			script: core.Script{
				Name:      "s",
				When:      core.ScriptEventTypeBefore,
				QueryFile: "/some/file.sql",
			},
		},
		{
			name: "valid with command",
			script: core.Script{
				Name:    "s",
				When:    core.ScriptEventTypeBefore,
				Command: []string{"echo", "hello"},
			},
		},
		{
			name: "invalid when value",
			script: core.Script{
				Name:  "s",
				When:  "always",
				Query: "SELECT 1",
			},
			wantErr:        true,
			wantErrContain: "validate 'when'",
		},
		{
			name: "no values set",
			script: core.Script{
				Name: "s",
				When: core.ScriptEventTypeBefore,
			},
			wantErr:        true,
			wantErrContain: "has no values",
		},
		{
			name: "more than one value set",
			script: core.Script{
				Name:      "s",
				When:      core.ScriptEventTypeBefore,
				Query:     "SELECT 1",
				QueryFile: "/some/file.sql",
			},
			wantErr:        true,
			wantErrContain: "has more than one value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewExecutor(tt.script)
			err := e.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- Executor.Exec dispatch ---

func TestExecutor_Exec(t *testing.T) {
	ctx := context.Background()

	t.Run("query dispatched to executeQuery", func(t *testing.T) {
		called := false
		exec := TxExec(func(_ context.Context, q string) error {
			called = true
			assert.Equal(t, "SELECT 42", q)
			return nil
		})
		e := NewExecutor(core.Script{Name: "s", Query: "SELECT 42"})
		require.NoError(t, e.Exec(ctx, exec))
		assert.True(t, called)
	})

	t.Run("query_file dispatched to executeQueryFile", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "*.sql")
		require.NoError(t, err)
		_, err = f.WriteString("SELECT 99")
		require.NoError(t, err)
		require.NoError(t, f.Close())

		called := false
		exec := TxExec(func(_ context.Context, q string) error {
			called = true
			assert.Equal(t, "SELECT 99", q)
			return nil
		})
		e := NewExecutor(core.Script{Name: "s", QueryFile: f.Name()})
		require.NoError(t, e.Exec(ctx, exec))
		assert.True(t, called)
	})

	t.Run("command dispatched to executeCommand", func(t *testing.T) {
		e := NewExecutor(core.Script{Name: "s", Command: []string{"echo", "hi"}})
		require.NoError(t, e.Exec(ctx, nil))
	})

	t.Run("nothing set returns errNothingToExecute", func(t *testing.T) {
		e := NewExecutor(core.Script{Name: "s"})
		err := e.Exec(ctx, nil)
		require.ErrorIs(t, err, errNothingToExecute)
	})
}

// --- executeQuery ---

func TestExecutor_executeQuery(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		execErr        error
		wantErr        bool
		wantErrContain string
	}{
		{
			name: "success",
		},
		{
			name:           "exec error wrapped with script name",
			execErr:        errors.New("db down"),
			wantErr:        true,
			wantErrContain: "execute script name='my-script'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewExecutor(core.Script{Name: "my-script", Query: "SELECT 1"})
			err := e.executeQuery(ctx, func(_ context.Context, _ string) error { return tt.execErr })
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- executeQueryFile ---

func TestExecutor_executeQueryFile(t *testing.T) {
	ctx := context.Background()

	t.Run("file not found", func(t *testing.T) {
		e := NewExecutor(core.Script{Name: "s", QueryFile: "/nonexistent/path/query.sql"})
		err := e.executeQueryFile(ctx, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot open script file")
	})

	t.Run("file content passed to exec", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "q.sql")
		require.NoError(t, os.WriteFile(path, []byte("SELECT 7"), 0600))

		var gotQuery string
		e := NewExecutor(core.Script{Name: "s", QueryFile: path})
		err := e.executeQueryFile(ctx, func(_ context.Context, q string) error {
			gotQuery = q
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, "SELECT 7", gotQuery)
	})

	t.Run("exec error wrapped with script name", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "q.sql")
		require.NoError(t, os.WriteFile(path, []byte("SELECT 1"), 0600))

		e := NewExecutor(core.Script{Name: "my-script", QueryFile: path})
		err := e.executeQueryFile(ctx, func(_ context.Context, _ string) error {
			return errors.New("exec failed")
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "execute script name='my-script'")
	})
}

// --- executeCommand ---

func TestExecutor_executeCommand(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		command        []string
		wantErr        bool
		wantErrContain string
	}{
		{
			name:    "valid command succeeds",
			command: []string{"echo", "hello"},
		},
		{
			name:           "unknown command fails",
			command:        []string{"unknown-command-that-does-not-exist-xyz"},
			wantErr:        true,
			wantErrContain: "execute script name='s'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewExecutor(core.Script{Name: "s", Command: tt.command})
			err := e.executeCommand(ctx)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContain)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
