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

package vendorutility

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

// fakeRunner is a utils.CmdRunnerInterface that writes canned content to the
// writer (or forwards) and records which execution mode was used.
type fakeRunner struct {
	stdout        string
	err           error
	forwarded     bool
	wroteToWriter bool
}

func (r *fakeRunner) ExecuteCmdAndForwardStdout(_ context.Context) error {
	r.forwarded = true
	return r.err
}

func (r *fakeRunner) ExecuteCmdAndWriteStdout(_ context.Context, w io.Writer) error {
	r.wroteToWriter = true
	if r.err != nil {
		return r.err
	}
	_, _ = io.WriteString(w, r.stdout)
	return nil
}

func (r *fakeRunner) ExecuteCmd(_ context.Context, _ io.Writer, _ int) error { return r.err }

// fakeProducer records the args it is asked to produce and returns a preset
// runner. produceErr lets tests exercise the produce-failure path.
type fakeProducer struct {
	runner     *fakeRunner
	produceErr error

	calls     int
	lastExe   string
	lastArgs  []string
	lastEnv   []string
	lastStdin io.Reader
}

func (p *fakeProducer) Produce(exe string, args, env []string, stdin io.Reader) (utils.CmdRunnerInterface, error) {
	p.calls++
	p.lastExe = exe
	p.lastArgs = args
	p.lastEnv = env
	p.lastStdin = stdin
	if p.produceErr != nil {
		return nil, p.produceErr
	}
	return p.runner, nil
}

// parseVer mimics an engine parser: the utility name is the leading token and
// the version follows "Ver ".
func parseVer(raw string) (name, version string) {
	if fields := strings.Fields(raw); len(fields) > 0 {
		name = fields[0]
	}
	_, rest, found := strings.Cut(raw, "Ver ")
	if !found {
		return name, ""
	}
	if sp := strings.IndexByte(rest, ' '); sp >= 0 {
		rest = rest[:sp]
	}
	return name, strings.TrimSpace(rest)
}

func TestCmdProvider_Version(t *testing.T) {
	runner := &fakeRunner{stdout: "mysqldump  Ver 8.0.35 for Linux on x86_64\n"}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "mysqldump", parseVer)

	vu, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "mysqldump", vu.Name)
	assert.Equal(t, "8.0.35", vu.VersionString)
	assert.Equal(t, []string{"8", "0", "35"}, vu.VersionParts)

	// The version is probed via "<exe> --version" with captured stdout.
	assert.Equal(t, "mysqldump", producer.lastExe)
	assert.Equal(t, []string{"--version"}, producer.lastArgs)
	assert.True(t, runner.wroteToWriter)
	assert.Nil(t, producer.lastStdin)
}

// TestCmdProvider_Version_nameFromOutput verifies the reported Name comes from
// the version output, not the (possibly path-like) executable.
func TestCmdProvider_Version_nameFromOutput(t *testing.T) {
	runner := &fakeRunner{stdout: "mysqldump  Ver 8.0.35 for Linux on x86_64\n"}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "/opt/mysql/bin/mysqldump", parseVer)

	vu, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "mysqldump", vu.Name)
	assert.Equal(t, "8.0.35", vu.VersionString)
	// The path is still what gets executed.
	assert.Equal(t, "/opt/mysql/bin/mysqldump", producer.lastExe)
}

// TestCmdProvider_Version_nameFallsBackToBasename verifies that when the output
// yields no name, Name falls back to the executable's base name (not the path).
func TestCmdProvider_Version_nameFallsBackToBasename(t *testing.T) {
	runner := &fakeRunner{stdout: "8.0.35\n"} // no leading utility name / "Ver"
	producer := &fakeProducer{runner: runner}
	p := New(producer, "/opt/mysql/bin/mysqldump", func(string) (string, string) {
		return "", "8.0.35"
	})

	vu, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "mysqldump", vu.Name)
	assert.Equal(t, "8.0.35", vu.VersionString)
}

func TestCmdProvider_Version_caches(t *testing.T) {
	runner := &fakeRunner{stdout: "mysqldump  Ver 8.0.35 for Linux\n"}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "mysqldump", parseVer)

	first, err := p.Version(context.Background())
	require.NoError(t, err)
	second, err := p.Version(context.Background())
	require.NoError(t, err)

	assert.Equal(t, first, second)
	// The successful probe is cached: the subprocess runs exactly once.
	assert.Equal(t, 1, producer.calls)
}

func TestCmdProvider_Version_errorNotCached(t *testing.T) {
	runner := &fakeRunner{err: errors.New("boom")}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "mysqldump", parseVer)

	_, err := p.Version(context.Background())
	require.Error(t, err)

	// A failed probe is not cached, so a later call retries.
	runner.err = nil
	runner.stdout = "mysqldump  Ver 8.0.35 for Linux\n"
	vu, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "8.0.35", vu.VersionString)
	assert.Equal(t, 2, producer.calls)
}

func TestCmdProvider_Version_produceError(t *testing.T) {
	producer := &fakeProducer{produceErr: errors.New("not found")}
	p := New(producer, "mysqldump", parseVer)

	_, err := p.Version(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "produce mysqldump version command")
}

func TestCmdProvider_Version_defaultArgs(t *testing.T) {
	runner := &fakeRunner{stdout: "mysqldump  Ver 8.0.35 for Linux\n"}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "mysqldump", parseVer)

	_, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []string{"--version"}, producer.lastArgs)
}

func TestCmdProvider_Version_withVersionArgs(t *testing.T) {
	runner := &fakeRunner{stdout: "some-tool Ver 1.2.3\n"}
	producer := &fakeProducer{runner: runner}
	p := New(producer, "some-tool", parseVer, WithVersionArgs("version", "--short"))

	vu, err := p.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "1.2.3", vu.VersionString)
	// The configured probe args are used verbatim instead of "--version".
	assert.Equal(t, []string{"version", "--short"}, producer.lastArgs)
}

func TestCmdProvider_Stream(t *testing.T) {
	t.Run("nil writer forwards stdout to log", func(t *testing.T) {
		runner := &fakeRunner{}
		producer := &fakeProducer{runner: runner}
		p := New(producer, "mysql", parseVer)

		stdin := strings.NewReader("-- schema")
		err := p.Stream(context.Background(), []string{"--host=localhost"}, []string{"E=1"}, stdin, nil)
		require.NoError(t, err)

		assert.True(t, runner.forwarded)
		assert.False(t, runner.wroteToWriter)
		assert.Equal(t, "mysql", producer.lastExe)
		assert.Equal(t, []string{"--host=localhost"}, producer.lastArgs)
		assert.Equal(t, []string{"E=1"}, producer.lastEnv)
		assert.Equal(t, stdin, producer.lastStdin)
	})

	t.Run("non-nil writer receives stdout", func(t *testing.T) {
		runner := &fakeRunner{stdout: "-- MySQL dump\n"}
		producer := &fakeProducer{runner: runner}
		p := New(producer, "mysqldump", parseVer)

		var buf strings.Builder
		err := p.Stream(context.Background(), []string{"shop"}, nil, nil, &buf)
		require.NoError(t, err)

		assert.True(t, runner.wroteToWriter)
		assert.False(t, runner.forwarded)
		assert.Equal(t, "-- MySQL dump\n", buf.String())
	})

	t.Run("produce error", func(t *testing.T) {
		producer := &fakeProducer{produceErr: errors.New("not found")}
		p := New(producer, "mysqldump", parseVer)

		err := p.Stream(context.Background(), nil, nil, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "produce mysqldump command")
	})
}
