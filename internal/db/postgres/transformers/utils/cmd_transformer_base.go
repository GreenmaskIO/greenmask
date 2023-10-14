package utils

import (
	"bufio"
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/rs/zerolog/log"
	"io"
	"os/exec"
	"syscall"
)

type CancelFunction func() error

type CmdTransformerBase struct {
	Name             string
	Cmd              *exec.Cmd
	ExpectedExitCode int
	Driver           *toolkit.Driver
	Api              DtoApi

	StdoutReader *bufio.Reader
	StderrReader *bufio.Reader
	StdinWriter  io.Writer
	Cancel       CancelFunction

	sendChan    chan struct{}
	receiveChan chan struct{}
}

func NewCmdTransformerBase(
	name string,
	expectedExitCode int,
	driver *toolkit.Driver,
	api DtoApi,
) *CmdTransformerBase {
	if api == nil {
		panic("api is nil")
	}

	return &CmdTransformerBase{
		Name:             name,
		ExpectedExitCode: expectedExitCode,
		Driver:           driver,
		Api:              api,
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
	if ctb.Api.SkipTransformation(r) {
		return r, nil
	}

	var err error
	var rd toolkit.RowDriver

	rd, err = ctb.Api.GetRowDriverFromRecord(r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error getting dto: %w", err)
	}

	err = ctb.Api.Encode(ctx, rd)
	if err != nil {
		return nil, fmt.Errorf("dto api error: cannot send tuple to transformer: %w", err)
	}

	res, err := ctb.Api.Decode(ctx)
	if err != nil {
		return nil, fmt.Errorf("dto api error: cannot receive transformed tuple from transformer: %w", err)
	}

	err = ctb.Api.SetRowDriverToRecord(res, r)
	if err != nil {
		return nil, fmt.Errorf("dto api error: error setting transfomed data to Record: %w", err)
	}

	return r, nil
}

func (ctb *CmdTransformerBase) BaseInitWithContext(ctx context.Context, executable string, args []string) error {
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

	ctb.Api.SetReader(ctb.StdoutReader)
	ctb.Api.SetWriter(ctb.StdinWriter)

	cancelFunction := func() error {
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
		line, _, err = ctb.StderrReader.ReadLine()
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
		line, _, err = ctb.StdoutReader.ReadLine()
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
