package custom

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

func GetDynamicTransformerDefinition(ctx context.Context, executable string, args ...string) (*CustomTransformerDefinition, error) {
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

	res := &CustomTransformerDefinition{}
	if err = json.Unmarshal(stdoutData, res); err != nil {
		log.Debug().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			Str("Output", string(stdoutData)).
			Msg("error unmarshalling custom transformer output")
		return nil, fmt.Errorf("error unmarshalling custom transformer output: %w", err)
	}
	if res.Mode != "" && res.Mode != JsonModeName && res.Mode != CsvModeName && res.Mode != TextModeName {
		return nil, fmt.Errorf(`error parsing transformer difinition: unknown mode name %s`, res.Mode)
	}
	if res.Mode == "" {
		res.Mode = JsonModeName
	}
	return res, nil
}
