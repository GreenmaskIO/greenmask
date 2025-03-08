package schema

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
)

type options interface {
	SchemaDumpParams() ([]string, error)
}

type storager interface {
	PutObject(ctx context.Context, filePath string, body io.Reader) error
}

type DumpCli struct {
	opt options
}

func NewDumpCli(opt options) *DumpCli {
	return &DumpCli{
		opt: opt,
	}
}

func (d *DumpCli) Run(ctx context.Context, st storager) error {
	params, err := d.opt.SchemaDumpParams()
	if err != nil {
		return fmt.Errorf("cannot get dump params: %w", err)
	}

	w, r := ioutils.NewGzipPipe(false)
	eg, gtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := st.PutObject(gtx, "schema.sql", r); err != nil {
			return fmt.Errorf("put schema schema.sql: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		defer w.Close()
		if err := cmdrunner.NewCmdRunner(d.binPath, params, w).Run(ctx); err != nil {
			return fmt.Errorf("cannot run mysqldump: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
