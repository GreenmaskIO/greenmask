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
	"context"
	"errors"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/restorers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
)

const exitCodeEnv = "GREENMASK_TEST_EXIT_CODE"

// TestHelperProcessExit is not a test: run as a subprocess with exitCodeEnv
// set, it exits with that code, which gives a real *exec.ExitError.
func TestHelperProcessExit(t *testing.T) {
	code := os.Getenv(exitCodeEnv)
	if code == "" {
		return
	}
	n, err := strconv.Atoi(code)
	if err != nil {
		os.Exit(100)
	}
	os.Exit(n)
}

func exitError(t *testing.T, code int) error {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcessExit$")
	cmd.Env = append(os.Environ(), exitCodeEnv+"="+strconv.Itoa(code))
	err := cmd.Run()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, code, exitErr.ExitCode())
	return err
}

func restoreWithExitOnError(exitOnError bool) *Restore {
	return &Restore{
		restoreOpt: &pgrestore.Options{ExitOnError: exitOnError},
		mx:         &sync.RWMutex{},
	}
}

type fakeTask struct{}

func (fakeTask) Execute(context.Context, utils.PGConnector) error { return nil }
func (fakeTask) DebugInfo() string                                { return `table "public"."orders"` }
func (fakeTask) GetEntry() *toc.Entry                             { return &toc.Entry{} }

func TestRestore_IgnoredErrorsResult_NoErrors(t *testing.T) {
	assert.NoError(t, restoreWithExitOnError(false).ignoredErrorsResult())
}

func TestRestore_IgnoredErrorsResult_LogsEachError(t *testing.T) {
	var buf bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&buf)
	t.Cleanup(func() { log.Logger = previous })

	r := restoreWithExitOnError(false)
	r.ignoredErrors = []error{errors.New("first failure"), errors.New("second failure")}

	assert.EqualError(t, r.ignoredErrorsResult(), "restore finished with 2 ignored error(s)")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	for i, want := range []string{"first failure", "second failure"} {
		assert.Contains(t, lines[i], `"level":"error"`)
		assert.Contains(t, lines[i], want)
	}
}

func TestRestore_HandlePgRestoreError(t *testing.T) {
	t.Run("errors ignored by pg_restore are recorded", func(t *testing.T) {
		r := restoreWithExitOnError(false)

		require.NoError(t, r.handlePgRestoreError("post-data", exitError(t, 1)))

		require.Len(t, r.ignoredErrors, 1)
		assert.Contains(t, r.ignoredErrors[0].Error(), "pg_restore reported errors in the post-data section")
		assert.EqualError(t, r.ignoredErrorsResult(), "restore finished with 1 ignored error(s)")
	})

	t.Run("other pg_restore failures stop the restore", func(t *testing.T) {
		r := restoreWithExitOnError(false)

		err := r.handlePgRestoreError("pre-data", exitError(t, 2))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot restore pre-data section using pg_restore")
		assert.NoError(t, r.ignoredErrorsResult())
	})

	t.Run("exit-on-error stops the restore", func(t *testing.T) {
		r := restoreWithExitOnError(true)

		require.Error(t, r.handlePgRestoreError("pre-data", exitError(t, 1)))
		assert.NoError(t, r.ignoredErrorsResult())
	})
}

func TestRestore_HandleTaskError(t *testing.T) {
	t.Run("an ignored table error is recorded and the worker carries on", func(t *testing.T) {
		r := restoreWithExitOnError(false)
		loadErr := errors.New("duplicate key value violates unique constraint")

		require.NoError(t, r.handleTaskError(fakeTask{}, 1, &restorers.IgnoredError{Err: loadErr}))

		require.Len(t, r.ignoredErrors, 1)
		assert.ErrorIs(t, r.ignoredErrors[0], loadErr)
		assert.Contains(t, r.ignoredErrors[0].Error(), `table "public"."orders"`)
		assert.EqualError(t, r.ignoredErrorsResult(), "restore finished with 1 ignored error(s)")
	})

	t.Run("other task errors stop the worker", func(t *testing.T) {
		r := restoreWithExitOnError(false)

		err := r.handleTaskError(fakeTask{}, 2, errors.New("cannot get storage object"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "worker 2")
		assert.NoError(t, r.ignoredErrorsResult())
	})

	t.Run("errors from several places are all reported", func(t *testing.T) {
		r := restoreWithExitOnError(false)

		require.NoError(t, r.handleTaskError(fakeTask{}, 1, &restorers.IgnoredError{Err: errors.New("a")}))
		require.NoError(t, r.handlePgRestoreError("post-data", exitError(t, 1)))

		assert.EqualError(t, r.ignoredErrorsResult(), "restore finished with 2 ignored error(s)")
	})
}
