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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var ErrRowTransformationTimeout = errors.New("row transformation timeout")

type CancelFunction func() error

type CmdTransformerBase struct {
	Name                     string
	Cmd                      *exec.Cmd
	ExpectedExitCode         int
	Driver                   *toolkit.Driver
	Api                      toolkit.InteractionApi
	ProcessedLines           int
	RowTransformationTimeout time.Duration

	StdoutReader *bufio.Reader
	StderrReader *bufio.Reader
	StdinWriter  io.Writer
	Cancel       CancelFunction

	sendChan    chan struct{}
	receiveChan chan struct{}
	opIsDone    chan struct{}
	terminated  bool
}

func NewCmdTransformerBase(
	name string,
	expectedExitCode int,
	rowTransformationTimeout time.Duration,
	driver *toolkit.Driver,
	api toolkit.InteractionApi,
) *CmdTransformerBase {
	if api == nil {
		panic("api is nil")
	}

	return &CmdTransformerBase{
		Name:                     name,
		ExpectedExitCode:         expectedExitCode,
		Driver:                   driver,
		Api:                      api,
		ProcessedLines:           -1,
		RowTransformationTimeout: rowTransformationTimeout,
		opIsDone:                 make(chan struct{}),
	}
}

func (ctb *CmdTransformerBase) BaseDone() error {
	log.Debug().
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("terminating custom transformer")

	if err := ctb.Cancel(); err != nil {
		log.Debug().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("error in termination function")
		return fmt.Errorf("error terminating custom transformer: %w", err)
	}

	log.Debug().
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("terminated successfully")
	return nil
}

func (ctb *CmdTransformerBase) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	ctb.ProcessedLines++
	var err error
	var rd toolkit.RowDriver
	ctx, cancel := context.WithTimeout(ctx, ctb.RowTransformationTimeout)
	defer cancel()

	rd, err = ctb.Api.GetRowDriverFromRecord(r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error getting dto: %w", err)
	}

	go func() {
		err = ctb.Api.Encode(ctx, rd)
		ctb.opIsDone <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrRowTransformationTimeout
		}
		return nil, ctx.Err()
	case <-ctb.opIsDone:
	}

	if err != nil {
		return nil, fmt.Errorf("interaction api error: cannot send tuple to transformer: %w", err)
	}

	go func() {
		rd, err = ctb.Api.Decode(ctx)
		ctb.opIsDone <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, ErrRowTransformationTimeout
		}
		return nil, ctx.Err()
	case <-ctb.opIsDone:
	}

	if err != nil {
		return nil, fmt.Errorf("interaction api error: cannot receive transformed tuple from transformer: %w", err)
	}

	err = ctb.Api.SetRowDriverToRecord(rd, r)
	if err != nil {
		return nil, fmt.Errorf("interaction api error: error setting transfomed data to record: %w", err)
	}

	ctb.Api.Clean()

	return r, nil
}

func (ctb *CmdTransformerBase) BaseInitWithContext(ctx context.Context, executable string, args []string) error {
	log.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("initializing transformer")

	ctb.Cmd = exec.CommandContext(ctx, executable, args...)
	if err := ctb.init(); err != nil {
		return err
	}
	ctb.Cmd.Cancel = ctb.Cancel
	if err := ctb.Cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("custom transformer exited with error")

		return fmt.Errorf("external command runtime error: %w", err)
	}
	return nil
}

func (ctb *CmdTransformerBase) BaseInit(executable string, args []string) error {
	log.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Str("TableSchema", ctb.Driver.Table.Schema).
		Str("TableName", ctb.Driver.Table.Name).
		Str("TransformerName", ctb.Name).
		Msg("initializing transformer")
	ctb.Cmd = exec.Command(executable, args...)
	if err := ctb.init(); err != nil {
		return err
	}
	if err := ctb.Cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("custom transformer exited with error")

		return fmt.Errorf("external command runtime error: %w", err)
	}
	return nil
}

func (ctb *CmdTransformerBase) init() error {

	ctb.sendChan = make(chan struct{}, 1)
	ctb.receiveChan = make(chan struct{}, 1)

	var err error
	stderr, err := ctb.Cmd.StderrPipe()
	if err != nil {
		return err
	}
	stdout, err := ctb.Cmd.StdoutPipe()
	if err != nil {
		stdout.Close()
		return err
	}

	stdin, err := ctb.Cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		stdout.Close()
		return err
	}
	ctb.StderrReader = bufio.NewReader(stderr)
	ctb.StdoutReader = bufio.NewReader(stdout)
	ctb.StdinWriter = stdin

	ctb.Api.SetReader(stdout)
	ctb.Api.SetWriter(stdin)

	cancelFunction := func() error {
		mx := &sync.Mutex{}
		mx.Lock()
		defer mx.Unlock()
		if ctb.terminated {
			return nil
		}
		ctb.terminated = true
		log.Debug().
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("running closing function")

		if ctb.Cmd.Process != nil && ctb.Cmd.ProcessState == nil ||
			ctb.Cmd.Process != nil && ctb.Cmd.ProcessState != nil && !ctb.Cmd.ProcessState.Exited() {
			log.Debug().
				Str("TableSchema", ctb.Driver.Table.Schema).
				Str("TableName", ctb.Driver.Table.Name).
				Str("TransformerName", ctb.Name).
				Int("TransformerPid", ctb.Cmd.Process.Pid).
				Msg("sending SIGTERM to custom transformer process")
			if err := ctb.Cmd.Process.Signal(syscall.SIGTERM); err != nil {
				log.Debug().
					Err(err).
					Str("TableSchema", ctb.Driver.Table.Schema).
					Str("TableName", ctb.Driver.Table.Name).
					Str("TransformerName", ctb.Name).
					Int("TransformerPid", ctb.Cmd.Process.Pid).
					Msg("error sending SIGTERM to custom transformer process")

				if ctb.Cmd.ProcessState != nil && !ctb.Cmd.ProcessState.Exited() {
					log.Warn().
						Str("TableSchema", ctb.Driver.Table.Schema).
						Str("TableName", ctb.Driver.Table.Name).
						Str("TransformerName", ctb.Name).
						Int("TransformerPid", ctb.Cmd.Process.Pid).
						Msg("killing process")
					if err = ctb.Cmd.Process.Kill(); err != nil {
						log.Warn().
							Err(err).
							Int("pid", ctb.Cmd.Process.Pid).
							Msg("error terminating custom transformer process")
					}
				}
			}
		}

		if err := stdin.Close(); err != nil {
			log.Debug().
				Str("TableSchema", ctb.Driver.Table.Schema).
				Str("TableName", ctb.Driver.Table.Name).
				Str("TransformerName", ctb.Name).
				Err(err).
				Msg("error closing stdin")
		}

		log.Debug().
			Str("TableSchema", ctb.Driver.Table.Schema).
			Str("TableName", ctb.Driver.Table.Name).
			Str("TransformerName", ctb.Name).
			Msg("closing function completed successfully")

		return nil
	}

	ctb.Cancel = cancelFunction

	return nil
}

func (ctb *CmdTransformerBase) ReceiveStderrLine(ctx context.Context) (line []byte, err error) {
	go func() {
		line, err = reader.ReadLine(ctb.StderrReader, nil)
		ctb.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-ctb.receiveChan:
	}

	if err != nil {
		return nil, err
	}

	return line, nil
}

func (ctb *CmdTransformerBase) ReceiveStdoutLine(ctx context.Context) (line []byte, err error) {
	go func() {
		line, err = reader.ReadLine(ctb.StdoutReader, nil)
		ctb.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-ctb.receiveChan:
	}

	if err != nil {
		return nil, err
	}

	return line, nil
}
