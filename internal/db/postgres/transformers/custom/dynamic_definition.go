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

package custom

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"

	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func GetDynamicTransformerDefinition(ctx context.Context, executable string, args ...string) (*TransformerDefinition, error) {
	log.Debug().
		Str("Executable", executable).
		Str("Args", strings.Join(args, " ")).
		Msg("performing autodiscovery")

	cmd := exec.Command(executable, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("error openning stdout pipe: %w", err)
	}
	defer stdout.Close()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("error openning stdout pipe: %w", err)
	}
	defer stderr.Close()
	if err != nil {
		log.Debug().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			Msg("error executing custom transformer: cannot get definition")
		return nil, fmt.Errorf("error executing custom transformer: %w", err)
	}
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("error running custom transformer: %w", err)
	}

	doneChan := make(chan struct{})
	var stdoutData, stderrData []byte
	eg := &errgroup.Group{}
	eg.Go(func() error {
		defer close(doneChan)
		var err error
		stdoutData, err = io.ReadAll(stdout)
		if err != nil {
			return fmt.Errorf("error reading stdout pipe: %w", err)
		}
		stderrData, err = io.ReadAll(stderr)
		if err != nil {
			return fmt.Errorf("error reading stderr pipe: %w", err)
		}
		if err = cmd.Wait(); err != nil {
			if len(stdoutData) > 0 {
				log.Info().
					Err(err).
					Str("Executable", executable).
					Str("Args", strings.Join(args, " ")).
					Msg("custom transformer stdout forwarding")

				buf := bufio.NewReader(bytes.NewBuffer(stdoutData))
				for {
					line, err := reader.ReadLine(buf)
					if err != nil {
						break
					}
					fmt.Printf("\tDATA: %s\n", string(line))
				}

			}
			if len(stderrData) > 0 {
				log.Info().
					Err(err).
					Str("Executable", executable).
					Str("Args", strings.Join(args, " ")).
					Msg("custom transformer stderr forwarding")

				buf := bufio.NewReader(bytes.NewBuffer(stderrData))
				for {
					line, err := reader.ReadLine(buf)
					if err != nil {
						break
					}
					fmt.Printf("\tDATA: %s\n", string(line))
				}
			}
			return fmt.Errorf("error running custom transformer: %w", err)
		}
		return nil
	})

	select {
	case <-ctx.Done():
		if ctx.Err() != nil {
			log.Warn().Err(err).Msg("error performing autodiscovery")
		}

		if cmd.ProcessState != nil && !cmd.ProcessState.Exited() {
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				log.Warn().
					Err(err).
					Int("TransformerPid", cmd.Process.Pid).
					Msg("error sending SIGTERM to custom transformer process")

				if cmd.ProcessState != nil && !cmd.ProcessState.Exited() {
					log.Warn().
						Int("TransformerPid", cmd.Process.Pid).
						Msg("killing process")
					if err = cmd.Process.Kill(); err != nil {
						log.Warn().
							Err(err).
							Int("TransformerPid", cmd.Process.Pid).
							Msg("error terminating custom transformer process")
					}
				}
			}
		}
		return nil, ctx.Err()
	case <-doneChan:
		log.Debug().Msg("transformer auto discovery: exited normally")
	}

	if err = eg.Wait(); err != nil {
		return nil, fmt.Errorf("error auto discover transformer: %w", err)
	}

	if len(stderrData) != 0 {
		log.Info().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			Msg("custom transformer stderr forwarding")
		fmt.Printf("\tDATA: %s\n", string(stderrData))
	}

	if len(stdoutData) == 0 {
		return nil, fmt.Errorf("received empty transformer definition: might be transfromer but or config mistake")
	}

	res := &TransformerDefinition{}
	if err = json.Unmarshal(stdoutData, res); err != nil {
		log.Debug().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			RawJSON("Output", stdoutData).
			Msg("error unmarshalling custom transformer output")
		return nil, fmt.Errorf("error unmarshalling custom transformer output: %w", err)
	}
	if res.Driver == nil {
		res.Driver = toolkit.DefaultRowDriverParams
	}
	if res.Driver.Name != "" && res.Driver.Name != JsonModeName && res.Driver.Name != CsvModeName && res.Driver.Name != TextModeName {
		return nil, fmt.Errorf(`error parsing transformer difinition: unknown mode name %s`, res.Driver.Name)
	}
	if res.Driver.Name == "" {
		res.Driver.Name = CsvModeName
	}
	return res, nil
}
