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

// CmdRunnerInterface defines the standard interface for executing commands
// that matches the existing CmdRunner struct.
type CmdRunnerInterface interface {
	// ExecuteCmdAndForwardStdout executes the command and forwards its stdout to the log.
	// It blocks until the command completes or the context is cancelled.
	ExecuteCmdAndForwardStdout(ctx context.Context) error
	// ExecuteCmdAndWriteStdout executes the command and writes its stdout to the provided writer.
	// It blocks until the command completes or the context is cancelled.
	// If the writer returns an error, the command execution will be terminated, and the error will be returned.
	ExecuteCmdAndWriteStdout(ctx context.Context, w io.Writer) error
	// ExecuteCmd executes the command using the specified mode (forwarder or storage writer).
	// It blocks until the command completes or the context is cancelled.
	// If a writer is provided and it returns an error, the command execution will be terminated,
	// and the error will be returned.
	ExecuteCmd(ctx context.Context, w io.Writer, mode int) error
}

// CmdProducer defines an interface for providing a CmdRunnerInterface instance
// initialized with the necessary parameters but without immediately running it.
type CmdProducer interface {
	Produce(executable string, args []string, env []string, stdin io.Reader) (CmdRunnerInterface, error)
}

// DefaultCmdProducer is a default implementation of the CmdProducer interface
// that returns the existing CmdRunner struct (which implements CmdRunnerInterface).
type DefaultCmdProducer struct{}

func NewDefaultCmdProducer() *DefaultCmdProducer {
	return &DefaultCmdProducer{}
}

func (d *DefaultCmdProducer) Produce(executable string, args []string, env []string, stdin io.Reader) (CmdRunnerInterface, error) {
	return NewCmdRunnerWithStdin(executable, args, env, stdin), nil
}

const (
	cmdRunnerStdoutReaderBufSize = 1024

	cmdModeStdoutStorageWriter = iota
	cmdModeStdoutForwarder
)

// CmdRunner is a struct that represents a command runner.
type CmdRunner struct {
	executable string
	args       []string
	env        []string
	stdin      io.Reader
}

func NewCmdRunner(executable string, args []string, env []string) *CmdRunner {
	return &CmdRunner{
		executable: executable,
		args:       args,
		env:        env,
	}
}

func NewCmdRunnerWithStdin(executable string, args []string, env []string, stdin io.Reader) *CmdRunner {
	return &CmdRunner{
		executable: executable,
		args:       args,
		env:        env,
		stdin:      stdin,
	}
}

func (c *CmdRunner) ExecuteCmdAndForwardStdout(ctx context.Context) error {
	return c.ExecuteCmd(ctx, nil, cmdModeStdoutForwarder)
}

func (c *CmdRunner) ExecuteCmdAndWriteStdout(ctx context.Context, w io.Writer) error {
	return c.ExecuteCmd(ctx, w, cmdModeStdoutStorageWriter)
}

// ExecuteCmd executes the command and writes stdout to the provided writer and logs stderr
// It returns an error if the command execution fails.
func (c *CmdRunner) ExecuteCmd(ctx context.Context, w io.Writer, mode int) error {
	log.Ctx(ctx).
		Debug().
		Str("Cmd", fmt.Sprintf("%s %s", path.Join(c.executable), strings.Join(c.args, " "))).
		Strs("Env", c.env).
		Msg("running command")
	cmd := exec.CommandContext(ctx, c.executable, c.args...)
	cmd.Env = append(cmd.Env, c.env...)

	errReader, errWriter := io.Pipe()
	outReader, outWriter := io.Pipe()

	if c.stdin != nil {
		cmd.Stdin = c.stdin
	}
	cmd.Stderr = errWriter
	cmd.Stdout = outWriter
	if err := cmd.Start(); err != nil {
		if err := errWriter.Close(); err != nil {
			log.Warn().Err(err).Msg("close stderr")
		}
		if err := outWriter.Close(); err != nil {
			log.Warn().Err(err).Msg("close stderr")
		}
		return fmt.Errorf("start external command: %w", err)
	}

	eg, gtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return c.listenStderrAndLog(gtx, errReader)
	})

	// stdout reader
	switch mode {
	case cmdModeStdoutForwarder:
		eg.Go(func() error {
			return c.listenStdoutAndLog(gtx, outReader)
		})
	case cmdModeStdoutStorageWriter:
		eg.Go(func() error {
			return c.listenStdoutAndWrite(gtx, outReader, w)
		})
	default:
		panic("unhandled default case")
	}

	eg.Go(func() error {
		// Wait for the command to finish and close the writer to signal the end of the stream.
		defer func() {
			if err := outWriter.Close(); err != nil {
				log.Warn().Err(err).Msg("close stdout")
			}
		}()

		defer func() {
			if err := errWriter.Close(); err != nil {
				log.Warn().Err(err).Msg("close stderr")
			}
		}()

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
func (c *CmdRunner) listenStdoutAndLog(ctx context.Context, errReader io.Reader) error {
	lineScanner := bufio.NewScanner(errReader)
	for lineScanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		log.Ctx(ctx).
			Info().
			Str("Executable", c.executable).
			Str("Stdout", lineScanner.Text()).
			Msg("stdout forwarding")
	}

	if err := lineScanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read stdout: %w", err)
	}
	return nil
}

// listenStderrAndLog reads from the provided reader and logs the stderr output.
func (c *CmdRunner) listenStderrAndLog(ctx context.Context, errReader io.Reader) error {
	lineScanner := bufio.NewScanner(errReader)
	for lineScanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		log.Ctx(ctx).
			Info().
			Str("Executable", c.executable).
			Str("Stderr", lineScanner.Text()).
			Msg("stderr forwarding")
	}

	if err := lineScanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read stderr: %w", err)
	}
	return nil
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
