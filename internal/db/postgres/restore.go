package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgrestore"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/restorers"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
	"golang.org/x/sync/errgroup"
)

type Restore struct {
	binPath string
	dsn     string
	options *pgrestore.Options
	st      storage.Storager
}

func NewRestore(binPath string, st storage.Storager) *Restore {
	return &Restore{
		binPath: binPath,
		st:      st,
	}
}

func (r *Restore) RunRestore(ctx context.Context, opt *pgrestore.Options, dumpId string) error {
	r.options = opt
	dsn, err := r.options.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cennot generate DSN: %w", err)
	}
	r.dsn = dsn
	pgRestore := pgrestore.NewPgRestore(r.binPath)
	files, dirs, err := r.st.ListDir(ctx)
	log.Debug().Msgf("%s", files)
	if err != nil {
		return fmt.Errorf("cannot walk through directory: %w", err)
	}
	var backupSt storage.Storager
	for _, item := range dirs {
		dirName, err := item.Dirname(ctx)
		if err != nil {
			return err
		}
		if dirName == dumpId {
			backupSt = item
			break
		}
	}
	if backupSt == nil {
		return fmt.Errorf("cannot find backup with id %s", dumpId)
	}

	var needUpload bool
	switch r.st.(type) {
	case *directory.Directory:
	default:
		needUpload = true
	}
	log.Debug().Msgf("needUpload = %s", needUpload)

	// TODO: Upload file to temp dir if needed
	dirname, err := backupSt.Getcwd(ctx)
	if err != nil {
		return fmt.Errorf("cannot get cwd for toc: %w", err)
	}
	options := *opt
	options.Section = "pre-data"
	options.DirPath = dirname
	if err = pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore pre-data section: %w", err)
	}

	tocFile, err := backupSt.GetReader(ctx, "toc.dat")
	if err != nil {
		return fmt.Errorf("cannot open toc file: %w", err)
	}

	ah, err := toc.ReadFile(tocFile)
	if err != nil {
		return fmt.Errorf("unable to read toc file: %w", err)
	}

	tasks := make(chan restorers.RestoreTask, r.options.Jobs)
	eg, gtx := errgroup.WithContext(ctx)
	for j := 0; j < r.options.Jobs; j++ {
		eg.Go(func(id int) func() error {
			return func() error {
				return r.restoreWorker(gtx, tasks, id+1)
			}
		}(j))
	}

	eg.Go(func() error {
		defer close(tasks)
		for _, entry := range ah.GetEntries() {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}

			if entry.Section == toc.SectionData {
				task := restorers.NewTableRestorer(entry, backupSt)
				select {
				case <-gtx.Done():
					return gtx.Err()
				case tasks <- task:

				}
			}

		}
		return nil
	})

	if err = eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}

	return nil
}

func (r *Restore) restoreWorker(ctx context.Context, tasks <-chan restorers.RestoreTask, id int) error {
	// TODO: You should execute TX for each COPY stmt
	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot connect to server: %w", err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for {
		var task restorers.RestoreTask
		select {
		case <-ctx.Done():
			log.Debug().Msgf("worker %d: restoring %s: existed due to cancelled context: %w", id, task.DebugInfo(), ctx.Err())
			return ctx.Err()
		case task = <-tasks:
			if task == nil {
				return nil
			}
		}

		if err := task.Execute(ctx, tx); err != nil {
			return fmt.Errorf("unable to perform restoration task: %w", err)
		}
	}
}
