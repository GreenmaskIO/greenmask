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

package utils

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"path"
	"strings"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const cmdRunnerStdoutReaderBufSize = 1024

// CmdRunner is a struct that represents a command runner.
type CmdRunner struct {
	executable string
	args       []string
}

func NewCmdRunner(executable string, args []string) *CmdRunner {
	return &CmdRunner{
		executable: executable,
		args:       args,
	}
}

// ExecuteCmdAndWriteStdout executes the command and writes stdout to the provided writer and logs stderr
// It returns an error if the command execution fails.
func (c *CmdRunner) ExecuteCmdAndWriteStdout(ctx context.Context, w io.Writer) error {
	log.Ctx(ctx).
		Debug().
		Str("Cmd", fmt.Sprintf("%s %s", path.Join(c.executable), strings.Join(c.args, " "))).
		Msg("running mysqldump")
	cmd := exec.CommandContext(ctx, c.executable, c.args...)

	errReader, errWriter := io.Pipe()
	defer errReader.Close()
	outReader, outWriter := io.Pipe()
	defer outWriter.Close()

	cmd.Stderr = errWriter
	cmd.Stdout = outWriter
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start external command: %w", err)
	}

	eg, gtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return c.listenStderrAndLog(gtx, errReader)
	})

	// stdout reader
	eg.Go(func() error {
		return c.listenStdoutAndWrite(gtx, outReader, w)
	})

	eg.Go(func() error {
		// Wait for the command to finish and close the writer to signal the end of the stream.
		defer outWriter.Close()
		defer errWriter.Close()
		if err := cmd.Wait(); err != nil {
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("execute command: %w", err)
	}

	return nil
}

// listenStderrAndLog reads from the provided reader and logs the stderr output.
func (c *CmdRunner) listenStderrAndLog(ctx context.Context, errReader io.Reader) error {
	lineScanner := bufio.NewReader(errReader)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		line, err := ReadLine(lineScanner, nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		log.Ctx(ctx).
			Info().
			Str("Executable", c.executable).
			Str("Stderr", string(line)).
			Msg("stderr forwarding")
	}
}

// listenStdoutAndWrite reads from the provided reader and writes to the provided writer.
// It uses for cmd call and dumps stdout to the storage. Let's say we have mysqldump schema output,
// and we want to dump it to the storage.
func (c *CmdRunner) listenStdoutAndWrite(ctx context.Context, stdout io.Reader, w io.Writer) error {
	buf := make([]byte, cmdRunnerStdoutReaderBufSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, err := stdout.Read(buf)
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				return fmt.Errorf("write stdout into object: %w", err)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}
