package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgrestore"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/restorers"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/storage/directory"
)

type Restore struct {
	binPath    string
	dsn        string
	options    *pgrestore.Options
	st         storage.Storager
	dumpIdList map[int32]bool
}

func NewRestore(binPath string, st storage.Storager) *Restore {
	return &Restore{
		binPath: binPath,
		st:      st,
	}
}

func (r *Restore) RunRestore(ctx context.Context, opt *pgrestore.Options, dumpId string) error {
	log.Info().Msgf("restoring dump %s", dumpId)
	r.options = opt
	if r.options.UseList != "" {
		// TODO: Implement toc entries ordering according to use-list
		log.Warn().Msgf("FIXME: Implement toc entries ordering according to use-list")
		if err := r.setRestoreList(r.options.UseList, r.options.ListFormat); err != nil {
			return fmt.Errorf("restore list parsing error: %w", err)
		}
	}
	dsn, err := r.options.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cennot generate DSN: %w", err)
	}
	r.dsn = dsn
	pgRestore := pgrestore.NewPgRestore(r.binPath)

	_, dirs, err := r.st.ListDir(ctx)
	if err != nil {
		return fmt.Errorf("cannot walk through directory: %w", err)
	}
	var backupSt storage.Storager
	for _, item := range dirs {
		if dumpId == item.Dirname() {
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
	options := *r.options
	options.Section = "pre-data"
	options.DirPath = dirname
	if err = pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore pre-data section using pg_restore: %w", err)
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

				if r.options.UseList != "" {
					_, apply := r.dumpIdList[entry.DumpId]
					if !apply {
						log.Debug().Msgf("toc entry was skipped dumpId=%d section=%s type=%s name=%s schema=%s",
							entry.DumpId, toc.SectionMap[entry.Section], *entry.Desc, *entry.Tag, *entry.Namespace)
						continue
					}
				}

				var task restorers.RestoreTask
				switch *entry.Desc {
				case domains.TableDataDesc:
					task = restorers.NewTableRestorer(entry, backupSt)
				case domains.SequenceSetDesc:
					task = restorers.NewSequenceRestorer(entry)
				case domains.LargeObjectDesc:
					log.Debug().Msgf("FIXME: Implement Large Object restoration")
				}

				if task != nil {
					select {
					case <-gtx.Done():
						return gtx.Err()
					case tasks <- task:
					}
				}
			}

		}
		return nil
	})

	if err = eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}

	options = *r.options
	options.Section = "post-data"
	options.DirPath = dirname
	if err = pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore post-data section using pg_restore: %w", err)
	}

	return nil
}

func (r *Restore) restoreWorker(ctx context.Context, tasks <-chan restorers.RestoreTask, id int) error {
	// TODO: You should execute TX for each COPY stmt
	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot connect to server (worker %d): %w", id, err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err).Msgf("cannot close worker connection to DB")
		}
	}()

	for {
		var task restorers.RestoreTask
		select {
		case <-ctx.Done():
			log.Debug().Msgf("existed due to cancelled context (worker %d restoring %s): %w", id, task.DebugInfo(), ctx.Err())
			return ctx.Err()
		case task = <-tasks:
			if task == nil {
				return nil
			}
		}
		log.Debug().Msgf("restoring %s (worker %d)", task.DebugInfo(), id)

		// Open new transaction for each task
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("cannot start transaction (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		if err = task.Execute(ctx, tx); err != nil {
			if txErr := tx.Rollback(ctx); txErr != nil {
				log.Warn().Msgf("cannot rollback transaction (worker %d restoring %s): %s", id, task.DebugInfo(), txErr)
			}
			return fmt.Errorf("unable to perform restoration task (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("cannot commit transaction (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		log.Debug().Msgf("restoration complete %s (worker %d)", task.DebugInfo(), id)
	}
}

func (r *Restore) setRestoreList(fileName string, format string) (err error) {
	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("unable to open list file: %w", err)
	}
	defer f.Close()
	var res map[int32]bool
	switch format {
	case "text":
		res, err = r.parseTextList(f)
	case "yaml":
		res, err = r.parseYamlList(f)
	case "json":
		res, err = r.parseJsonList(f)
	}
	if err != nil {
		r.dumpIdList = res
	}
	return err
}

func (r *Restore) parseTextList(f *os.File) (map[int32]bool, error) {
	const dumpIdGroup = 1
	var lineNumber int
	var lineBuf = make([]byte, 0, 1024)
	buf := bytes.NewBuffer(lineBuf)
	lr := bufio.NewReader(f)
	pattern, err := regexp.Compile(`^\s*(?P<DumpId>\d+)\s*;.*$`)
	if err != nil {
		return nil, fmt.Errorf("cannot compile regexp: %s", err)
	}
	res := make(map[int32]bool, 0)
	for {
		line, isPrefix, err := lr.ReadLine()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return res, nil
			}
			return nil, fmt.Errorf("read line error: %w", err)
		}
		buf.Write(line)
		if isPrefix {
			continue
		}
		lineNumber++
		found := pattern.FindStringSubmatch(buf.String())
		if len(found) != 2 {
			log.Debug().Msgf("text list parser: skipped line %d", lineNumber)
			buf.Reset()
			continue
		}
		dumpId, err := strconv.ParseInt(found[dumpIdGroup], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("cannot parse dumpId at line %d", lineNumber)
		}
		res[int32(dumpId)] = true
		buf.Reset()
	}
}

func (r *Restore) parseYamlList(f *os.File) (map[int32]bool, error) {
	meta := &domains.Metadata{}
	if err := yaml.NewDecoder(f).Decode(meta); err != nil {
		return nil, fmt.Errorf("metadata parsing error: %w", err)
	}
	res := make(map[int32]bool, 0)
	for idx, entry := range meta.Entries {
		if entry.DumpId == 0 {
			return nil, fmt.Errorf("broken list file dumpId: must not be 0: entry number %d", idx)
		}
		res[entry.DumpId] = true
	}
	return res, nil
}

func (r *Restore) parseJsonList(f *os.File) (map[int32]bool, error) {
	meta := &domains.Metadata{}
	if err := json.NewDecoder(f).Decode(meta); err != nil {
		return nil, fmt.Errorf("metadata parsing error: %w", err)
	}
	res := make(map[int32]bool, 0)
	for idx, entry := range meta.Entries {
		if entry.DumpId == 0 {
			return nil, fmt.Errorf("broken list file dumpId: must not be 0: entry number %d", idx)
		}
		res[entry.DumpId] = true
	}
	return res, nil
}
