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

package pgrestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/utils/cmd_runner"
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
	defer func() {
		if err := f.Close(); err != nil {
			log.Debug().Err(err).Msg("error closing query file")
		}
	}()
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
