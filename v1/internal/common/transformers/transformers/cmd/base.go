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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
)

type TransformerBase struct {
	Name             string
	Cmd              *exec.Cmd
	ExpectedExitCode int
	TableName        string
	TableSchema      string
	//Driver                   interfaces.TableDriver
	Proto                    CMDProto
	ProcessedLines           int
	RowTransformationTimeout time.Duration

	StdoutReader *bufio.Reader
	StderrReader *bufio.Reader
	StdinWriter  io.Writer
	Cancel       func() error

	sendChan    chan struct{}
	receiveChan chan struct{}
	opIsDone    chan struct{}
	terminated  bool
}

func NewTransformerBase(
	name string,
	expectedExitCode int,
	rowTransformationTimeout time.Duration,
	table *commonmodels.Table,
	proto CMDProto,
) *TransformerBase {
	return &TransformerBase{
		Name:                     name,
		ExpectedExitCode:         expectedExitCode,
		TableName:                table.Name,
		TableSchema:              table.Schema,
		Proto:                    proto,
		ProcessedLines:           -1,
		RowTransformationTimeout: rowTransformationTimeout,
		opIsDone:                 make(chan struct{}),
	}
}

func (t *TransformerBase) Done(ctx context.Context) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", t.TableName).
		Str("TableName", t.TableSchema).
		Str("TransformerName", t.Name).
		Logger()
	logger.Debug().Msg("terminating custom transformer")

	if err := t.Cancel(); err != nil {
		logger.Debug().Msg("error in termination function")
		return fmt.Errorf("error terminating custom transformer: %w", err)
	}

	logger.Debug().Msg("terminated successfully")
	return nil
}

func (t *TransformerBase) Transform(ctx context.Context, r interfaces.Recorder) error {
	t.ProcessedLines++
	// It might be too expensive to have a timeout for each row transformation,
	// but we need to protect from hanging transformers.
	ctx, cancel := context.WithTimeout(ctx, t.RowTransformationTimeout)
	defer cancel()

	if err := t.Proto.Send(ctx, r); err != nil {
		return fmt.Errorf("send tuple: %w", err)
	}

	if err := t.Proto.ReceiveAndApply(ctx, r); err != nil {
		err = fmt.Errorf("receive and apply transformed tuple: %w", err)
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("transformer process closed the stream unexpectedly: %w", err)
		}
		return err
	}

	return nil
}

func (t *TransformerBase) Init(ctx context.Context, executable string, args []string) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", t.TableName).
		Str("TableName", t.TableSchema).
		Str("TransformerName", t.Name).
		Logger()
	logger.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Msg("initializing transformer")

	t.Cmd = exec.CommandContext(ctx, executable, args...)
	if err := t.init(ctx); err != nil {
		return err
	}
	t.Cmd.Cancel = t.Cancel
	if err := t.Cmd.Start(); err != nil {
		logger.Warn().Err(err).Msg("custom transformer exited with error")

		return fmt.Errorf("external command runtime error: %w", err)
	}

	return nil
}

func (t *TransformerBase) init(ctx context.Context) error {
	logger := log.Ctx(ctx).With().
		Str("TableSchema", t.TableName).
		Str("TableName", t.TableSchema).
		Str("TransformerName", t.Name).
		Logger()

	t.sendChan = make(chan struct{}, 1)
	t.receiveChan = make(chan struct{}, 1)

	var err error
	stderr, err := t.Cmd.StderrPipe()
	if err != nil {
		return err
	}
	stdout, err := t.Cmd.StdoutPipe()
	if err != nil {
		stdout.Close()
		return err
	}

	stdin, err := t.Cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		stdout.Close()
		return err
	}
	t.StderrReader = bufio.NewReader(stderr)
	t.StdoutReader = bufio.NewReader(stdout)
	t.StdinWriter = stdin

	if err := t.Proto.Init(stdin, stdout); err != nil {
		return fmt.Errorf("initializing protocol: %w", err)
	}

	cancelFunction := func() error {
		if t.terminated {
			return nil
		}
		t.terminated = true
		logger.Debug().Msg("running closing function")

		if t.Cmd.Process != nil && t.Cmd.ProcessState == nil ||
			t.Cmd.Process != nil && t.Cmd.ProcessState != nil && !t.Cmd.ProcessState.Exited() {
			logger.Debug().
				Int("TransformerPid", t.Cmd.Process.Pid).
				Msg("sending SIGTERM to custom transformer process")
			if err := t.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
				logger.Debug().
					Int("TransformerPid", t.Cmd.Process.Pid).
					Msg("error sending SIGTERM to custom transformer process")

				if t.Cmd.ProcessState != nil && !t.Cmd.ProcessState.Exited() {
					logger.Debug().
						Int("TransformerPid", t.Cmd.Process.Pid).
						Msg("killing process")
					if err = t.Cmd.Process.Kill(); err != nil {
						logger.Warn().Err(err).
							Int("pid", t.Cmd.Process.Pid).
							Msg("error terminating custom transformer process")
					}
				}
			}
		}

		if err := stdin.Close(); err != nil {
			logger.Debug().Err(err).Msg("error closing stdin")
		}

		logger.Debug().Msg("closing function completed successfully")

		return nil
	}

	t.Cancel = cancelFunction

	return nil
}

func (t *TransformerBase) ReceiveStderrLine(ctx context.Context) (line []byte, err error) {
	go func() {
		line, err = utils.ReadLine(t.StderrReader, nil)
		t.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-t.receiveChan:
	}

	if err != nil {
		return nil, err
	}

	return line, nil
}

func (t *TransformerBase) ReceiveStdoutLine(ctx context.Context) (line []byte, err error) {
	go func() {
		line, err = utils.ReadLine(t.StdoutReader, nil)
		t.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-t.receiveChan:
	}

	if err != nil {
		return nil, err
	}

	return line, nil
}
