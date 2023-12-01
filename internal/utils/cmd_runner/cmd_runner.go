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
			logger.Info().Str("Executable", name).Str("Stderr", string(line)).Msg("stderr forwarding")
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
			logger.Info().Str("Executable", name).Str("Stdout", string(line)).Msg("stderr forwarding")
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
