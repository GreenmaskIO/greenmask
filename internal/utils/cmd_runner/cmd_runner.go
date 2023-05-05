package cmd_runner

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"

	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
)

func Run(ctx context.Context, logger *zerolog.Logger, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)

	errReader, errWriter := io.Pipe()
	defer errReader.Close()
	outReader, outWriter := io.Pipe()
	defer outWriter.Close()

	cmd.Stderr = errWriter
	cmd.Stdout = outWriter
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("external command runtime error: %w", err)
	}

	eg, gtx := errgroup.WithContext(ctx)

	// stderr reader
	eg.Go(func() error {
		lineScanner := bufio.NewReader(errReader)
		for {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}
			line, _, err := lineScanner.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			logger.Info().Msg(string(line))
		}
	})

	// stdout reader
	eg.Go(func() error {
		lineScanner := bufio.NewReader(outReader)
		for {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}
			line, _, err := lineScanner.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			logger.Info().Msg(string(line))
		}
	})

	eg.Go(func() error {
		defer outWriter.Close()
		defer errWriter.Close()
		if err := cmd.Wait(); err != nil {
			return fmt.Errorf("external command runtime error: %w", err)
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("cannot execute command: %w", err)
	}

	return nil
}
