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

// Package vendorutility provides a generic, engine-agnostic implementation of
// core.VendorUtilityProvider that wraps utils.CmdProducer. Engine packages
// construct it with their executable name and a version parser (e.g. mysqldump,
// mysql, pg_dump) and inject it into their schema dumpers/restorers.
package vendorutility

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

var _ core.VendorUtilityProvider = (*CmdProvider)(nil)

// defaultVersionArgs is the version-probe argument used when the caller does not
// supply its own via WithVersionArgs. Most vendor CLIs (mysqldump, mysql,
// pg_dump…) accept "--version", but the exact flag is configurable because it is
// not universal.
var defaultVersionArgs = []string{"--version"}

// Option configures a CmdProvider.
type Option func(*CmdProvider)

// WithVerbose records the caller's intent to run the utility verbosely. The
// concrete verbose flags are vendor-specific and supplied by the caller through
// Stream args; the provider only enables debug logging around its invocations.
func WithVerbose() Option {
	return func(p *CmdProvider) { p.verbose = true }
}

// WithVersionArgs overrides the arguments used to probe the utility version.
// Defaults to {"--version"}. Use it for utilities that expose their version
// under a different flag (e.g. {"version"} or {"-V"}).
func WithVersionArgs(args ...string) Option {
	return func(p *CmdProvider) { p.versionArgs = args }
}

// CmdProvider is the generic VendorUtilityProvider built on top of
// utils.CmdProducer. It owns the executable (a name or a path) and a parse
// function, caches the probed version, and runs the utility either forwarding
// stdout to the log or streaming it to a writer.
type CmdProvider struct {
	cmd        utils.CmdProducer
	executable string
	// parse extracts the utility's self-reported name and version token out of
	// the version-probe output (e.g. "mysqldump  Ver 8.0.35 …" -> "mysqldump",
	// "8.0.35").
	parse       func(raw string) (name, version string)
	versionArgs []string
	verbose     bool

	mu     sync.Mutex
	cached *core.VendorUtility
}

// New builds a CmdProvider for executable (a bare name or a filesystem path),
// using parse to extract the utility name and version token out of the
// version-probe output. The probe arguments default to {"--version"} and can be
// overridden with WithVersionArgs.
func New(cmd utils.CmdProducer, executable string, parse func(raw string) (name, version string), opts ...Option) *CmdProvider {
	p := &CmdProvider{
		cmd:         cmd,
		executable:  executable,
		parse:       parse,
		versionArgs: defaultVersionArgs,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *CmdProvider) Name() string { return p.executable }

// Version probes the utility version (running "<executable> <versionArgs...>"),
// capturing stdout into a buffer and parsing the version token out of it. The
// result is cached on success so repeated calls do not re-spawn the subprocess;
// a failed probe is not cached so it can be retried.
func (p *CmdProvider) Version(ctx context.Context) (core.VendorUtility, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cached != nil {
		return *p.cached, nil
	}

	runner, err := p.cmd.Produce(p.executable, p.versionArgs, nil, nil)
	if err != nil {
		return core.VendorUtility{}, fmt.Errorf("produce %s version command: %w", p.executable, err)
	}
	var buf bytes.Buffer
	if err := runner.ExecuteCmdAndWriteStdout(ctx, &buf); err != nil {
		return core.VendorUtility{}, fmt.Errorf("probe %s version: %w", p.executable, err)
	}

	name, versionString := p.parse(buf.String())
	// The executable may be a path (e.g. "/opt/mysql/bin/mysqldump"); prefer the
	// name the utility reports about itself, falling back to the executable's
	// base name when the probe output yields none.
	if name == "" {
		name = filepath.Base(p.executable)
	}
	var versionParts []string
	if versionString != "" {
		versionParts = strings.Split(versionString, ".")
	}
	vu := core.VendorUtility{
		Name:          name,
		VersionString: versionString,
		VersionParts:  versionParts,
	}
	if p.verbose {
		log.Ctx(ctx).Debug().
			Str("Executable", p.executable).
			Str("Name", vu.Name).
			Str("Version", vu.VersionString).
			Str("Raw", buf.String()).
			Msg("probed vendor utility version")
	}
	p.cached = &vu
	return vu, nil
}

// Stream runs the utility with args/env. When w is nil stdout is forwarded to
// the log (schema restore); otherwise it is streamed to w (schema dump).
func (p *CmdProvider) Stream(ctx context.Context, args, env []string, stdin io.Reader, w io.Writer) error {
	runner, err := p.cmd.Produce(p.executable, args, env, stdin)
	if err != nil {
		return fmt.Errorf("produce %s command: %w", p.executable, err)
	}
	if w == nil {
		return runner.ExecuteCmdAndForwardStdout(ctx)
	}
	return runner.ExecuteCmdAndWriteStdout(ctx, w)
}
