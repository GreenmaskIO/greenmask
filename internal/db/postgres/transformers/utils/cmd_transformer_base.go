package utils

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var ErrRowTransformationTimeout = errors.New("row transformation timeout")

type CancelFunction func() error

type CmdTransformerBase struct {
	name                  string
	executable            string
	args                  []string
	cmd                   *exec.Cmd
	expectedExitCode      int
	transformationTimeout time.Duration
	driver                *toolkit.Driver
	Api                   DtoApi

	stdoutReader *bufio.Reader
	stderrReader *bufio.Reader
	stdinWriter  io.Writer
	cancel       CancelFunction
	sendChan     chan struct{}
	receiveChan  chan struct{}
	t            *time.Ticker
}

func NewCmdTransformerBase(
	name string,
	executable string, args []string, expectedExitCode int,
	transformationTimeout time.Duration, driver *toolkit.Driver,
	api DtoApi,
) (*CmdTransformerBase, error) {
	if api == nil {
		panic("api is nil")
	}

	return &CmdTransformerBase{
		name:                  name,
		executable:            executable,
		args:                  args,
		expectedExitCode:      expectedExitCode,
		transformationTimeout: transformationTimeout,
		driver:                driver,
		t:                     time.NewTicker(transformationTimeout),
		Api:                   api,
	}, nil
}

func (ctb *CmdTransformerBase) Done() error {
	log.Debug().
		Str("TableSchema", ctb.driver.Table.Schema).
		Str("TableName", ctb.driver.Table.Name).
		Str("TransformerName", ctb.name).
		Msg("terminating custom transformer")

	if err := ctb.cancel(); err != nil {
		log.Debug().
			Err(err).
			Str("TableSchema", ctb.driver.Table.Schema).
			Str("TableName", ctb.driver.Table.Name).
			Str("TransformerName", ctb.name).
			Msg("error in termination function")
		return fmt.Errorf("error terminating custom transformer: %w", err)
	}

	log.Debug().
		Str("TableSchema", ctb.driver.Table.Schema).
		Str("TableName", ctb.driver.Table.Name).
		Str("TransformerName", ctb.name).
		Msg("terminated successfully")
	return nil
}

func (ctb *CmdTransformerBase) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	if ctb.Api.Skip(r) {
		return r, nil
	}

	ctb.t.Reset(ctb.transformationTimeout)
	var err error
	var rd toolkit.RowDriver

	rd, err = ctb.Api.GetRowDriverFromRecord(r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error getting dto: %w", err)
	}

	if err = ctb.SendOriginalTuple(ctx, rd); err != nil {
		return nil, fmt.Errorf("cannot send tuple to transformer: %w", err)
	}

	transformedData, err := ctb.ReceiveTransformedTuple(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot receive transformed tuple from transformer: %w", err)
	}

	rd, err = ctb.Api.Unmarshal(transformedData)
	if err != nil {
		return nil, fmt.Errorf("dto api error: unmarshalling error: %w", err)
	}

	err = ctb.Api.SetRowDriverToRecord(rd, r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error setting transfomed data to Record: %w", err)
	}

	return r, nil
}

func (ctb *CmdTransformerBase) BaseInit(ctx context.Context, args []string) (CancelFunction, error) {
	log.Debug().
		Str("executable", ctb.executable).
		Str("args", strings.Join(args, " ")).
		Msg("running custom transformer")

	ctb.cmd = exec.CommandContext(ctx, ctb.executable, args...)
	ctb.sendChan = make(chan struct{}, 1)
	ctb.receiveChan = make(chan struct{}, 1)

	var err error
	stderr, err := ctb.cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := ctb.cmd.StdoutPipe()
	if err != nil {
		stdout.Close()
		return nil, err
	}

	stdin, err := ctb.cmd.StdinPipe()
	if err != nil {
		stderr.Close()
		stdout.Close()
		return nil, err
	}
	ctb.stderrReader = bufio.NewReader(stderr)
	ctb.stdoutReader = bufio.NewReader(stdout)
	ctb.stdinWriter = stdin

	cancelFunction := func() error {
		log.Debug().
			Str("TableSchema", ctb.driver.Table.Schema).
			Str("TableName", ctb.driver.Table.Name).
			Str("TransformerName", ctb.name).
			Msg("running closing function")

		if ctb.cmd.Process != nil && ctb.cmd.ProcessState == nil ||
			ctb.cmd.Process != nil && ctb.cmd.ProcessState != nil && !ctb.cmd.ProcessState.Exited() {
			log.Debug().
				Str("TableSchema", ctb.driver.Table.Schema).
				Str("TableName", ctb.driver.Table.Name).
				Str("TransformerName", ctb.name).
				Int("TransformerPid", ctb.cmd.Process.Pid).
				Msg("sending SIGTERM to custom transformer process")
			if err := ctb.cmd.Process.Signal(syscall.SIGTERM); err != nil {
				log.Debug().
					Err(err).
					Str("TableSchema", ctb.driver.Table.Schema).
					Str("TableName", ctb.driver.Table.Name).
					Str("TransformerName", ctb.name).
					Int("TransformerPid", ctb.cmd.Process.Pid).
					Msg("error sending SIGTERM to custom transformer process")

				if ctb.cmd.ProcessState != nil && !ctb.cmd.ProcessState.Exited() {
					log.Warn().
						Str("TableSchema", ctb.driver.Table.Schema).
						Str("TableName", ctb.driver.Table.Name).
						Str("TransformerName", ctb.name).
						Int("TransformerPid", ctb.cmd.Process.Pid).
						Msg("killing process")
					if err = ctb.cmd.Process.Kill(); err != nil {
						log.Warn().
							Err(err).
							Int("pid", ctb.cmd.Process.Pid).
							Msg("error terminating custom transformer process")
					}
				}
			}
		}

		if err := stdin.Close(); err != nil {
			log.Debug().
				Str("TableSchema", ctb.driver.Table.Schema).
				Str("TableName", ctb.driver.Table.Name).
				Str("TransformerName", ctb.name).
				Err(err).
				Msg("error closing stdin")
		}

		log.Debug().
			Str("TableSchema", ctb.driver.Table.Schema).
			Str("TableName", ctb.driver.Table.Name).
			Str("TransformerName", ctb.name).
			Msg("closing function completed successfully")

		return nil
	}

	ctb.cmd.Cancel = cancelFunction

	if err := ctb.cmd.Start(); err != nil {
		log.Warn().
			Err(err).
			Str("TableSchema", ctb.driver.Table.Schema).
			Str("TableName", ctb.driver.Table.Name).
			Str("TransformerName", ctb.name).
			Msg("custom transformer exited with error")

		return nil, fmt.Errorf("external command runtime error: %w", err)
	}

	return cancelFunction, nil
}

func (ctb *CmdTransformerBase) ReceiveStderrLine(ctx context.Context) (line []byte, err error) {
	go func() {
		line, _, err = ctb.stderrReader.ReadLine()
		ctb.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, nil
	case <-ctb.receiveChan:
	}

	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
			return nil, nil
		}
		log.Debug().Err(err).Msg("line reader error")
		return nil, err
	}

	return line, nil
}

func (ctb *CmdTransformerBase) SendOriginalTuple(ctx context.Context, rawRecord toolkit.RowDriver) (err error) {

	go func() {
		err = ctb.Api.Encode(rawRecord, ctb.stdinWriter)
		ctb.sendChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ctb.t.C:
		return ErrRowTransformationTimeout
	case <-ctb.sendChan:

	}
	if err != nil {
		return err
	}
	return nil
}

func (ctb *CmdTransformerBase) ReceiveTransformedTuple(ctx context.Context) (line []byte, err error) {
	go func() {
		line, _, err = ctb.stdoutReader.ReadLine()
		ctb.receiveChan <- struct{}{}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ctb.t.C:
		return nil, ErrRowTransformationTimeout
	case <-ctb.receiveChan:
	}

	if err != nil {
		return nil, fmt.Errorf("error receiving data from transformer: %w", err)
	}
	return line, nil
}
