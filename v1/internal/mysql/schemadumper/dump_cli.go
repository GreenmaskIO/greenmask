package schemadumper

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const executable = "mysqldump"

type options interface {
	SchemaDumpParams() ([]string, error)
}

type DumpCli struct {
	executable string
	opt        options
}

func NewDumpCli(opt options) *DumpCli {
	return &DumpCli{
		executable: executable,
		opt:        opt,
	}
}

func (d *DumpCli) Run(ctx context.Context, st storages.Storager) error {
	params, err := d.opt.SchemaDumpParams()
	if err != nil {
		return fmt.Errorf("cannot get dump params: %w", err)
	}
	w, r := utils.NewGzipPipe(false)
	eg, gtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		if err := st.PutObject(gtx, "schema.sql", r); err != nil {
			return fmt.Errorf("put schema schema.sql: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		defer func(w utils.CountWriteCloser) {
			_ = w.Close()
		}(w)
		if err := utils.NewCmdRunner(d.executable, params).ExecuteCmdAndWriteStdout(ctx, w); err != nil {
			return fmt.Errorf("cannot run mysqldump: %w", err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	return nil
}
