package script

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/rs/zerolog/log"
)

type Scheduler struct {
	scripts []core.Script
}

func NewScheduler(scripts []core.Script) *Scheduler {
	return &Scheduler{
		scripts: scripts,
	}
}

func (s *Scheduler) Exec(
	ctx context.Context,
	session core.DatabaseSession,
	currentSection core.DumpSection,
	currentWhen core.ScriptEventType,
) error {
	for i := range s.scripts {
		script := s.scripts[i]
		if script.Section != currentSection || script.When != currentWhen {
			continue
		}
		if err := NewExecutor(script).Exec(ctx, session); err != nil {
			return fmt.Errorf("execute script #%d: %w", i, err)
		}
	}
	return nil
}

var errNothingToExecute = errors.New("nothing to execute")

type Executor struct {
	core.Script
}

func NewExecutor(script core.Script) *Executor {
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

func (s *Executor) Exec(ctx context.Context, session core.DatabaseSession) error {
	switch {
	case s.Query != "":
		return s.executeQuery(ctx, session)
	case s.QueryFile != "":
		return s.executeQueryFile(ctx, session)
	case len(s.Command) > 0:
		return s.executeCommand(ctx)
	default:
		return errNothingToExecute
	}
}

// execQuery applies a single query to the restore session, delegating the
// transaction lifecycle to core.ExecOnSession.
func (s *Executor) execQuery(ctx context.Context, session core.DatabaseSession, query string) error {
	err := core.ExecOnSession(ctx, session, func(ctx context.Context, db core.DB) error {
		_, err := db.ExecContext(ctx, query)
		return err
	})
	if err != nil {
		return fmt.Errorf("execute script name='%s': %w", s.Name, err)
	}
	return nil
}

func (s *Executor) executeQuery(ctx context.Context, session core.DatabaseSession) error {
	return s.execQuery(ctx, session, s.Query)
}

func (s *Executor) executeQueryFile(ctx context.Context, session core.DatabaseSession) error {
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
	return s.execQuery(ctx, session, string(query))
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
