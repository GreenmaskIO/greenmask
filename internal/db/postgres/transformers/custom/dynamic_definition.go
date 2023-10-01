package custom

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"io"
	"os/exec"
	"strings"
)

func GetDynamicTransformerDefinition(ctx context.Context, executable string, args ...string) (*transformers.CustomTransformerDefinition, error) {
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

	ctx, cancel := context.WithTimeout(ctx, AutoDiscoveryTimeout)
	defer cancel()

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
	case <-doneChan:
		log.Debug().Msg("transformer auto discovery: exited normally")
	case <-ctx.Done():
		if ctx.Err() != nil {
			log.Warn().Err(err).Msg("error performing autodiscovery")
		}
		err = cmd.Process.Kill()
		if err != nil {
			log.Warn().Err(err).Msg("error killing atotransformer")
		}
	}

	if err = eg.Wait(); err != nil {
		return nil, fmt.Errorf("error auto discover transformer: %w", err)
	}

	if len(stderrData) != 0 {
		log.Info().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			Str("Stderr", string(stderrData)).
			Msg("custom transformer stderr forwarding")
	}

	if len(stdoutData) == 0 {
		return nil, fmt.Errorf("received empty transformer definition: might be transfromer but or config mistake")
	}

	res := &transformers.CustomTransformerDefinition{}
	if err = json.Unmarshal(stdoutData, res); err != nil {
		log.Debug().
			Err(err).
			Str("Executable", executable).
			Str("Args", strings.Join(args, " ")).
			Str("Output", string(stdoutData)).
			Msg("error unmarshalling custom transformer output")
		return nil, fmt.Errorf("error unmarshalling custom transformer output: %w", err)
	}
	return res, nil
}
