package script

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/rs/zerolog/log"
)

type Scheduler struct {
	scripts []commonmodels.Script
}

func NewScheduler(scripts []commonmodels.Script) *Scheduler {
	return &Scheduler{
		scripts: scripts,
	}
}

func (s *Scheduler) Exec(
	ctx context.Context,
	exec TxExec,
	currentSection commonmodels.DumpSection,
	currentWhen commonmodels.ScriptEventType,
) error {
	for i := range s.scripts {
		script := s.scripts[i]
		if script.Section != currentSection || script.When != currentWhen {
			continue
		}
		if err := NewExecutor(script).Exec(ctx, exec); err != nil {
			return fmt.Errorf("execute script #%d: %w", i, err)
		}
	}
	return nil
}

type TxExec func(ctx context.Context, query string) error

// TxExecBuilder opens an engine-specific DB connection for script execution.
// Returning (nil, no-op, nil) is valid — it means no SQL scripts will run.
type TxExecBuilder func(ctx context.Context) (TxExec, func(), error)

var errNothingToExecute = errors.New("nothing to execute")

type Executor struct {
	commonmodels.Script
}

func NewExecutor(script commonmodels.Script) *Executor {
	return &Executor{
		Script: script,
	}
}

func (s *Executor) Validate() error {
	if err := s.When.Validate(); err != nil {
		return fmt.Errorf("validate 'when': %w", err)
	}

	values := []string{s.Query, s.QueryFile, strings.Join(s.Command, " ")}
	var count int
	for _, value := range values {
		if value != "" {
			count += 1
		}
	}
	if count == 0 {
		return fmt.Errorf("script '%s' has no values", s.Name)
	}
	if count > 1 {
		return fmt.Errorf("script '%s' has more than one value", s.Name)
	}
	return nil
}

func (s *Executor) Exec(ctx context.Context, exec TxExec) error {
	switch {
	case s.Query != "":
		return s.executeQuery(ctx, exec)
	case s.QueryFile != "":
		return s.executeQueryFile(ctx, exec)
	case len(s.Command) > 0:
		return s.executeCommand(ctx)
	default:
		return errNothingToExecute
	}
}

func (s *Executor) executeQuery(ctx context.Context, exec TxExec) error {
	if err := exec(ctx, s.Query); err != nil {
		return fmt.Errorf("execute script name='%s': %w", s.Name, err)
	}
	return nil
}

func (s *Executor) executeQueryFile(ctx context.Context, exec TxExec) error {
	f, err := os.Open(s.QueryFile)
	defer func() {
		if err := f.Close(); err != nil {
			log.Ctx(ctx).Debug().Err(err).Msg("error closing query file")
		}
	}()
	if err != nil {
		return fmt.Errorf("cannot open script file: %w", err)
	}
	query, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("cannot read query file: %w", err)
	}
	if err := exec(ctx, string(query)); err != nil {
		return fmt.Errorf("execute script name='%s': %w", s.Name, err)
	}
	return err
}

func (s *Executor) executeCommand(ctx context.Context) error {
	log.Ctx(ctx).Debug().
		Str("exec", s.Command[0]).
		Str("args", strings.Join(s.Command[1:], " ")).
		Msg("executing script")
	cmd := utils.NewCmdRunner(s.Command[0], s.Command[1:], os.Environ())
	if err := cmd.ExecuteCmdAndForwardStdout(ctx); err != nil {
		return fmt.Errorf("execute script name='%s': %w", s.Name, err)
	}
	return nil
}
