package schema

import (
	"context"
	"fmt"
	"io"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const executable = "mysqldump"

type options interface {
	SchemaDumpParams() ([]string, error)
	Env() ([]string, error)
}

type Dumper struct {
	executable string
	opt        options
	st         storages.Storager
}

func New(st storages.Storager, opt options) *Dumper {
	return &Dumper{
		executable: executable,
		opt:        opt,
		st:         st,
	}
}

func (d *Dumper) DumpSchema(ctx context.Context) error {
	env, err := d.opt.Env()
	if err != nil {
		return fmt.Errorf("getting environment variables: %w", err)
	}
	params, err := d.opt.SchemaDumpParams()
	if err != nil {
		return fmt.Errorf("cannot get dump params: %w", err)
	}
	r, w := io.Pipe()
	eg, gtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := d.st.PutObject(gtx, "schema.sql", r); err != nil {
			return fmt.Errorf("put schema schema.sql: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		defer func(w io.Closer) {
			if err := w.Close(); err != nil {
				log.Ctx(ctx).Error().
					Str("Stage", "SchemaDump").
					Msgf("closing output writer: %v", err)
			}
		}(w)
		if err := utils.NewCmdRunner(d.executable, params, env).ExecuteCmdAndWriteStdout(ctx, w); err != nil {
			return fmt.Errorf("cannot run mysqldump: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
