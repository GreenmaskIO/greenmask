// Copyright 2025 Greenmask
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

package schema

import (
	"context"
	"fmt"
	"io"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	schemaFileName = "schema.sql"

	executable = "mysql"
)

type options interface {
	SchemaRestoreParams() ([]string, error)
	Env() ([]string, error)
}

// Restorer - restores mysql schema from the given storage folder.
// It expected that schema DDL commands are stored in schema.sql.
type Restorer struct {
	st         storages.Storager
	cfg        options
	executable string
}

func NewRestorer(
	st storages.Storager,
	connCfg options,
) *Restorer {
	return &Restorer{
		st:         st,
		cfg:        connCfg,
		executable: executable,
	}
}

func (r *Restorer) restoreSchemaData(ctx context.Context, f io.Reader) error {
	params, err := r.cfg.SchemaRestoreParams()
	if err != nil {
		return fmt.Errorf("get schema restore params: %w", err)
	}
	env, err := r.cfg.Env()
	if err != nil {
		return fmt.Errorf("get schema restore env: %w", err)
	}
	cmd := utils.NewCmdRunnerWithStdin(r.executable, params, env, f)

	if err := cmd.ExecuteCmdAndForwardStdout(ctx); err != nil {
		return fmt.Errorf("execute schema restore command: %w", err)
	}
	return nil
}

func (r *Restorer) RestoreSchema(ctx context.Context) error {
	f, err := r.st.GetObject(ctx, schemaFileName)
	if err != nil {
		return fmt.Errorf("get schema file from storage: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("close schema file")
		}
	}()
	if err := r.restoreSchemaData(ctx, f); err != nil {
		return fmt.Errorf("restore schema data: %w", err)
	}
	return nil
}
