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

package cmd

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/restorers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
)

const (
	ScriptPreDataSection  = "pre-data"
	ScriptDataSection     = "data"
	ScriptPostDataSection = "post-data"
)

const (
	ScriptExecuteBefore = "before"
	ScriptExecuteAfter  = "after"
)

type Restore struct {
	binPath    string
	dsn        string
	scripts    map[string][]pgrestore.Script
	pgRestore  *pgrestore.PgRestore
	restoreOpt *pgrestore.Options
	st         storages.Storager
	dumpIdList map[int32]bool
	tocObj     *toc.Toc
	tmpDir     string
}

func NewRestore(
	binPath string, st storages.Storager, opt *pgrestore.Options, s map[string][]pgrestore.Script, tmpDir string,
) *Restore {

	return &Restore{
		binPath:    binPath,
		st:         st,
		pgRestore:  pgrestore.NewPgRestore(binPath),
		restoreOpt: opt,
		scripts:    s,
		tmpDir:     path.Join(tmpDir, fmt.Sprintf("%d", time.Now().UnixNano())),
	}
}

func (r *Restore) RunScripts(ctx context.Context, conn *pgx.Conn, section, when string) error {
	if section != ScriptPreDataSection &&
		section != ScriptDataSection && section != ScriptPostDataSection {
		return fmt.Errorf(`unknown "section" value: %s`, section)
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
				return fmt.Errorf(`cannot aply script "%s" %s %s section: %w`, script.Name, when, section, err)
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

	if err := os.Mkdir(r.tmpDir, 0700); err != nil {
		return fmt.Errorf("error creating temp dir: %w", err)
	}

	tocFile, err := r.st.GetObject(ctx, "toc.dat")
	if err != nil {
		return fmt.Errorf("cannot open toc file: %w", err)
	}
	defer tocFile.Close()

	tmpTocFile, err := os.Create(path.Join(r.tmpDir, "toc.dat"))
	if err != nil {
		return fmt.Errorf("error creating temp to file in tmpDir: %w", err)
	}
	defer tmpTocFile.Close()

	if _, err = io.Copy(tmpTocFile, tocFile); err != nil {
		return fmt.Errorf("error uploading toc file to tmpDir: %w", err)
	}
	if _, err = tmpTocFile.Seek(0, 0); err != nil {
		return fmt.Errorf("unnable to move toc file offset to the head: %w", err)
	}
	tocReader := toc.NewReader(tmpTocFile)
	r.tocObj, err = tocReader.Read()
	if err != nil {
		return fmt.Errorf("unable to read toc file: %w", err)
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
	options.DirPath = r.tmpDir
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
		for _, entry := range r.tocObj.Entries {
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
				case toc.TableDataDesc:
					task = restorers.NewTableRestorer(entry, r.st)
				case toc.SequenceSetDesc:
					task = restorers.NewSequenceRestorer(entry)
				case toc.BlobsDesc:
					task = restorers.NewBlobsRestorer(entry, r.st)
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
	options.DirPath = r.tmpDir
	if err := r.pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore post-data section using pg_restore: %w", err)
	}

	if err := r.RunScripts(ctx, conn, ScriptPostDataSection, ScriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) prune() {
	if err := os.RemoveAll(r.tmpDir); err != nil {
		log.Debug().Err(err).Msg("error deleting temp dir")
	}
}

func (r *Restore) Run(ctx context.Context) error {

	defer r.prune()

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
			Msgf("restoration complete")
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
	pattern, err := regexp.Compile(`^\s*(?P<DumpIdSequence>\d+)\s*;.*$`)
	if err != nil {
		return nil, fmt.Errorf("cannot compile regexp: %s", err)
	}
	res := make(map[int32]bool)
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
	meta := &storage.Metadata{}
	if err := yaml.NewDecoder(f).Decode(meta); err != nil {
		return nil, fmt.Errorf("metadata parsing error: %w", err)
	}
	res := make(map[int32]bool)
	for idx, entry := range meta.Entries {
		if entry.DumpId == 0 {
			return nil, fmt.Errorf("broken list file dumpId: must not be 0: entry number %d", idx)
		}
		res[entry.DumpId] = true
	}
	return res, nil
}

func (r *Restore) parseJsonList(f *os.File) (map[int32]bool, error) {
	meta := &storage.Metadata{}
	if err := json.NewDecoder(f).Decode(meta); err != nil {
		return nil, fmt.Errorf("metadata parsing error: %w", err)
	}
	res := make(map[int32]bool)
	for idx, entry := range meta.Entries {
		if entry.DumpId == 0 {
			return nil, fmt.Errorf("broken list file dumpId: must not be 0: entry number %d", idx)
		}
		res[entry.DumpId] = true
	}
	return res, nil
}
