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
