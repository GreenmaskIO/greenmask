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

const (
	ScriptPreFlightSection  = "pre-flight"
	ScriptPreDataSection    = "pre-data"
	ScriptDataSection       = "data"
	ScriptPostDataSection   = "post-data"
	ScriptPostFlightSection = "post-flight"
)

const (
	ScriptExecuteBefore = "before"
	ScriptExecuteAfter  = "after"
)

type Restore struct {
	binPath    string
	dsn        string
	scripts    map[string][]domains.Script
	pgRestore  *pgrestore.PgRestore
	restoreOpt *pgrestore.Options
	st         storage.Storager
	dumpIdList map[int32]bool
	conn       *pgx.Conn
	ah         *toc.ArchiveHandle
	dumpSt     storage.Storager
}

func NewRestore(binPath string, st storage.Storager, opt *pgrestore.Options, s map[string][]domains.Script) *Restore {

	return &Restore{
		binPath:    binPath,
		st:         st,
		pgRestore:  pgrestore.NewPgRestore(binPath),
		restoreOpt: opt,
		scripts:    s,
	}
}

func (r *Restore) RunScripts(ctx context.Context, conn *pgx.Conn, section, when string) error {
	if section != ScriptPreFlightSection && section != ScriptPreDataSection &&
		section != ScriptDataSection && section != ScriptPostDataSection &&
		section != ScriptPostFlightSection {
		return fmt.Errorf(`unknown "section" value: %s`, section)
	}
	if section != ScriptPreFlightSection && section != ScriptPostFlightSection &&
		when != ScriptExecuteBefore && when != ScriptExecuteAfter {
		return fmt.Errorf(`unknown "when" value: %s`, when)
	}
	if r.scripts == nil {
		return nil
	}
	scripts, ok := r.scripts[section]
	if !ok {
		return nil
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction: %w", err)
	}
	defer func() {
		if err := tx.Commit(ctx); err != nil {
			log.Warn().Err(err).Msgf("cannot commit transaction")
		}
	}()
	for _, script := range scripts {
		if script.When == when {
			log.Info().
				Str("section", section).
				Str("when", when).
				Str("script", script.Name).
				Msgf("executing script")
			if err := script.Execute(ctx, tx); err != nil {
				return fmt.Errorf("cannot aply %s %s script %s: %w", when, section, script.Name, err)
			}
			log.Info().
				Str("section", section).
				Str("when", when).
				Str("script", script.Name).
				Msgf("script execution complete")
		}
	}
	return nil
}

func (r *Restore) uploadTocFile(ctx context.Context) error {
	var needUpload bool
	switch r.st.(type) {
	case *directory.Directory:
	default:
		needUpload = true
	}
	log.Debug().Msgf("needUpload = %s", needUpload)
	if needUpload {
		return fmt.Errorf("toc file uploading does not implemented")
	}
	return nil
}

func (r *Restore) prepare() error {
	if r.restoreOpt.UseList != "" {
		// TODO: Implement toc entries ordering according to use-list
		log.Warn().Msgf("FIXME: Implement toc entries ordering according to use-list")
		if err := r.setRestoreList(r.restoreOpt.UseList, r.restoreOpt.ListFormat); err != nil {
			return fmt.Errorf("restore list parsing error: %w", err)
		}
	}
	dsn, err := r.restoreOpt.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cennot generate DSN: %w", err)
	}
	r.dsn = dsn
	return nil
}

func (r *Restore) preFlightRestore(ctx context.Context, conn *pgx.Conn) error {
	// TODO: Upload file to temp dir if needed
	if err := r.uploadTocFile(ctx); err != nil {
		return err
	}
	tocFile, err := r.st.GetReader(ctx, "toc.dat")
	if err != nil {
		return fmt.Errorf("cannot open toc file: %w", err)
	}
	defer tocFile.Close()
	ah, err := toc.ReadFile(tocFile)
	if err != nil {
		return fmt.Errorf("unable to read toc file: %w", err)
	}
	r.ah = ah

	// Execute PreFlight scripts
	if err = r.RunScripts(ctx, conn, ScriptPreFlightSection, ""); err != nil {
		return err
	}

	return nil
}

func (r *Restore) preDataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Check restore options
	if r.restoreOpt.DataOnly ||
		r.restoreOpt.Section != "" && r.restoreOpt.Section != ScriptPreDataSection {
		return nil
	}

	// Execute PreData Before scripts
	if err := r.RunScripts(ctx, conn, ScriptPreDataSection, ScriptExecuteBefore); err != nil {
		return err
	}

	// Execute pre-data section restore using pg_restore
	options := *r.restoreOpt
	options.Section = "pre-data"
	options.DirPath = r.st.Getcwd()
	if err := r.pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore pre-data section using pg_restore: %w", err)
	}

	// Execute PreData After scripts
	if err := r.RunScripts(ctx, conn, ScriptPreDataSection, ScriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) dataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Execute Data Before scripts

	if r.restoreOpt.SchemaOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != ScriptDataSection) {
		return nil
	}

	if err := r.RunScripts(ctx, conn, ScriptDataSection, ScriptExecuteBefore); err != nil {
		return err
	}

	tasks := make(chan restorers.RestoreTask, r.restoreOpt.Jobs)
	eg, gtx := errgroup.WithContext(ctx)
	for j := 0; j < r.restoreOpt.Jobs; j++ {
		eg.Go(func(id int) func() error {
			return func() error {
				return r.restoreWorker(gtx, tasks, id+1)
			}
		}(j))
	}

	eg.Go(func() error {
		defer close(tasks)
		for _, entry := range r.ah.GetEntries() {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}

			if entry.Section == toc.SectionData {

				if r.restoreOpt.UseList != "" {
					_, apply := r.dumpIdList[entry.DumpId]
					if !apply {
						log.Info().
							Int32("dumpId", entry.DumpId).
							Str("section", toc.SectionMap[entry.Section]).
							Str("type", *entry.Desc).
							Str("name", *entry.Tag).
							Str("schema", *entry.Namespace).
							Msg("toc entry was skipped")
						continue
					}
				}

				var task restorers.RestoreTask
				switch *entry.Desc {
				case domains.TableDataDesc:
					task = restorers.NewTableRestorer(entry, r.st)
				case domains.SequenceSetDesc:
					task = restorers.NewSequenceRestorer(entry)
				case domains.LargeObjectDesc:
					log.Warn().Msgf("FIXME: Implement Large Object restoration")
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

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}

	// Execute Data After scripts
	if err := r.RunScripts(ctx, conn, ScriptDataSection, ScriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) postDataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Execute Post Data Before scripts

	if r.restoreOpt.DataOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != ScriptPostDataSection) {
		return nil
	}

	if err := r.RunScripts(ctx, conn, ScriptPostDataSection, ScriptExecuteBefore); err != nil {
		return err
	}

	options := *r.restoreOpt
	options.Section = "post-data"
	options.DirPath = r.st.Getcwd()
	if err := r.pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore post-data section using pg_restore: %w", err)
	}

	if err := r.RunScripts(ctx, conn, ScriptPostDataSection, ScriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) postFlightRestore(ctx context.Context, conn *pgx.Conn) error {
	if err := r.RunScripts(ctx, conn, ScriptPostFlightSection, ""); err != nil {
		return err
	}
	return nil
}

func (r *Restore) RunRestore(ctx context.Context) error {

	if err := r.prepare(); err != nil {
		return fmt.Errorf("preparation error: %w", err)
	}

	// Establish connection for scripts
	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot establish connection to db: %w", err)
	}
	defer conn.Close(ctx)

	if err = r.preFlightRestore(ctx, conn); err != nil {
		return fmt.Errorf("pre-flight stage restoration error: %w", err)
	}

	if err = r.preDataRestore(ctx, conn); err != nil {
		return fmt.Errorf("pre-data stage restoration error: %w", err)
	}

	if err = r.dataRestore(ctx, conn); err != nil {
		return fmt.Errorf("data stage restoration error: %w", err)
	}

	if err = r.postDataRestore(ctx, conn); err != nil {
		return fmt.Errorf("post-data stage restoration error: %w", err)
	}

	if err = r.postFlightRestore(ctx, conn); err != nil {
		return fmt.Errorf("post-data stage restoration error: %w", err)
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
			log.Debug().
				Int("workerId", id).
				Str("objectName", task.DebugInfo()).
				Err(ctx.Err()).
				Msg("existed due to cancelled context")
			return ctx.Err()
		case task = <-tasks:
			if task == nil {
				return nil
			}
		}
		log.Debug().
			Int("workerId", id).
			Str("objectName", task.DebugInfo()).
			Msg("restoring")

		// Open new transaction for each task
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("cannot start transaction (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		if err = task.Execute(ctx, tx); err != nil {
			if txErr := tx.Rollback(ctx); txErr != nil {
				log.Warn().
					Err(txErr).
					Int("workerId", id).
					Str("objectName", task.DebugInfo()).
					Msg("cannot rollback transaction")
			}
			return fmt.Errorf("unable to perform restoration task (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		if err = tx.Commit(ctx); err != nil {
			return fmt.Errorf("cannot commit transaction (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		log.Debug().
			Int("workerId", id).
			Str("objectName", task.DebugInfo()).
			Msgf("restoration completed")
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
			log.Debug().
				Str("parser", "text list parser").
				Msgf("skipped line %d", lineNumber)
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
