package custom

import (
	"encoding/json"
	"fmt"
	"github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
	"github.com/rs/zerolog/log"
	"io"
	"os/exec"
	"strings"
)

func GetDynamicTransformerDefinition(executable string, args ...string) (*transformers.CustomTransformerDefinition, error) {
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
	stdoutData, err := io.ReadAll(stdout)
	stderrData, err := io.ReadAll(stderr)

	if err = cmd.Wait(); err != nil {
		return nil, fmt.Errorf("error running custom transformer: %w", err)
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
