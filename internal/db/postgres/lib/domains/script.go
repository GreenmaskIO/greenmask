package domains

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/utils/cmd_runner"
)

type Script struct {
	Name      string   `mapstructure:"name"`
	When      string   `mapstructure:"when"`
	Query     string   `mapstructure:"query"`
	QueryFile string   `mapstructure:"query_file"`
	Command   []string `mapstructure:"command"`
}

func (s *Script) ExecuteQuery(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, s.Query)
	return err
}

func (s *Script) ExecuteQueryFile(ctx context.Context, tx pgx.Tx) error {
	f, err := os.Open(s.QueryFile)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("cannot open script file: %w", err)
	}
	query, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("cannot read query file: %w", err)
	}
	_, err = tx.Exec(ctx, string(query))
	return err
}

func (s *Script) ExecuteCommand(ctx context.Context) error {
	log.Debug().
		Str("exec", s.Command[0]).
		Str("args", strings.Join(s.Command[1:], " ")).
		Msg("executing script")
	return cmd_runner.Run(ctx, &log.Logger, s.Command[0], s.Command[1:]...)
}

func (s *Script) Execute(ctx context.Context, tx pgx.Tx) error {
	if s.Query != "" {
		return s.ExecuteQuery(ctx, tx)
	} else if s.QueryFile != "" {
		return s.ExecuteQueryFile(ctx, tx)
	} else if len(s.Command) > 0 {
		return s.ExecuteCommand(ctx)
	} else {
		return errors.New("nothing to execute")
	}
}
