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
	"slices"
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
	scriptPreDataSection  = "pre-data"
	scriptDataSection     = "data"
	scriptPostDataSection = "post-data"
)

const (
	preDataSection  = "pre-data"
	dataSection     = "data"
	postDataSection = "post-data"
)

const (
	scriptExecuteBefore = "before"
	scriptExecuteAfter  = "after"
)

const (
	jsonListFormat = "json"
	yamlListFormat = "yaml"
	textListFormat = "text"
)

type Restore struct {
	binPath    string
	dsn        string
	scripts    map[string][]pgrestore.Script
	pgRestore  *pgrestore.PgRestore
	restoreOpt *pgrestore.Options
	st         storages.Storager
	dumpIdList []int32
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
	if section != scriptPreDataSection &&
		section != scriptDataSection && section != scriptPostDataSection {
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
	if err := os.Mkdir(r.tmpDir, 0700); err != nil {
		return fmt.Errorf("error creating temp dir: %w", err)
	}

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

	if r.dumpIdList != nil {
		if err = r.sortAndFilterEntriesByRestoreList(); err != nil {
			return fmt.Errorf("unable to sort entries by the provided list: %w", err)
		}
	}

	if len(r.restoreOpt.Schema) > 0 {
		for idx, name := range r.restoreOpt.Schema {
			r.restoreOpt.Schema[idx] = removeEscapeQuotes(name)
		}
	}

	if len(r.restoreOpt.Table) > 0 {
		for idx, name := range r.restoreOpt.Table {
			r.restoreOpt.Table[idx] = removeEscapeQuotes(name)
		}
	}

	if len(r.restoreOpt.ExcludeSchema) > 0 {
		for idx, name := range r.restoreOpt.ExcludeSchema {
			r.restoreOpt.ExcludeSchema[idx] = removeEscapeQuotes(name)
		}
	}

	return nil
}

func (r *Restore) sortAndFilterEntriesByRestoreList() error {
	sortedEntries := make([]*toc.Entry, len(r.dumpIdList))

	for idx, dumpId := range r.dumpIdList {
		foundIdx := slices.IndexFunc(r.tocObj.Entries, func(entry *toc.Entry) bool {
			return entry.DumpId == dumpId
		})
		if foundIdx == -1 {
			return fmt.Errorf("entry from provided list with dump id %d is not found", dumpId)
		}
		sortedEntries[idx] = r.tocObj.Entries[foundIdx]
	}
	r.tocObj.Entries = sortedEntries
	return nil
}

func (r *Restore) preDataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Do not restore this section if implicitly provided
	if r.restoreOpt.DataOnly ||
		r.restoreOpt.Section != "" && r.restoreOpt.Section != preDataSection {
		return nil
	}

	// Execute PreData Before scripts
	if err := r.RunScripts(ctx, conn, scriptPreDataSection, scriptExecuteBefore); err != nil {
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
	if err := r.RunScripts(ctx, conn, scriptPreDataSection, scriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) dataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Execute Data Before scripts

	// Do not restore this section if implicitly provided
	if r.restoreOpt.SchemaOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != dataSection) {
		return nil
	}

	if err := r.RunScripts(ctx, conn, scriptDataSection, scriptExecuteBefore); err != nil {
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

				if !r.isNeedRestore(entry) {
					continue
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
	if err := r.RunScripts(ctx, conn, scriptDataSection, scriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) isNeedRestore(e *toc.Entry) bool {

	if *e.Desc == toc.TableDataDesc || *e.Desc == toc.SequenceSetDesc {

		if len(r.restoreOpt.ExcludeSchema) > 0 &&
			slices.Contains(r.restoreOpt.ExcludeSchema, removeEscapeQuotes(*e.Namespace)) {

			return true
		}

		if len(r.restoreOpt.Schema) > 0 &&
			!slices.Contains(r.restoreOpt.Schema, removeEscapeQuotes(*e.Namespace)) {

			return false
		}

		if len(r.restoreOpt.Table) > 0 &&
			!slices.Contains(r.restoreOpt.Table, removeEscapeQuotes(*e.Tag)) {

			return false
		}

		return true
	}

	return true
}

func (r *Restore) postDataRestore(ctx context.Context, conn *pgx.Conn) error {
	// Execute Post Data Before scripts

	// Do not restore this section if implicitly provided
	if r.restoreOpt.DataOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != postDataSection) {
		return nil
	}

	if err := r.RunScripts(ctx, conn, scriptPostDataSection, scriptExecuteBefore); err != nil {
		return err
	}

	options := *r.restoreOpt
	options.Section = "post-data"
	options.DirPath = r.tmpDir
	if err := r.pgRestore.Run(ctx, &options); err != nil {
		return fmt.Errorf("cannot restore post-data section using pg_restore: %w", err)
	}

	if err := r.RunScripts(ctx, conn, scriptPostDataSection, scriptExecuteAfter); err != nil {
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
	switch format {
	case textListFormat:
		r.dumpIdList, err = r.parseTextList(f)
	case yamlListFormat, jsonListFormat:
		r.dumpIdList, err = r.parseStructuredList(f, format)
	}
	return err
}

func (r *Restore) parseTextList(f *os.File) ([]int32, error) {
	const dumpIdGroup = 1
	var lineNumber int
	var lineBuf = make([]byte, 0, 1024)
	buf := bytes.NewBuffer(lineBuf)
	lr := bufio.NewReader(f)
	pattern, err := regexp.Compile(`^\s*(?P<DumpIdSequence>\d+)\s*;.*$`)
	if err != nil {
		return nil, fmt.Errorf("cannot compile regexp: %s", err)
	}
	//res := make(map[int32]bool)
	var res []int32
	idx := 0
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
		res = append(res, int32(dumpId))
		buf.Reset()
		idx++
	}
}

func (r *Restore) parseStructuredList(f *os.File, format string) ([]int32, error) {
	meta := &storage.Metadata{}

	switch format {
	case jsonListFormat:
		if err := json.NewDecoder(f).Decode(meta); err != nil {
			return nil, fmt.Errorf("metadata parsing error in json format: %w", err)
		}
	case yamlListFormat:
		if err := yaml.NewDecoder(f).Decode(meta); err != nil {
			return nil, fmt.Errorf("metadata parsing error in yaml format: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown format %s", format)
	}

	// Build entries by the provided list and create temporal file for pg_restore call

	r.restoreOpt.UseList = path.Join(r.tmpDir, "restoration.list")
	tmpListFile, err := os.Create(r.restoreOpt.UseList)
	if err != nil {
		return nil, fmt.Errorf("unable to create temporal use-list file: %w", err)
	}
	defer tmpListFile.Close()

	var res []int32
	for idx, entry := range meta.Entries {
		if entry.DumpId == 0 {
			return nil, fmt.Errorf("broken list file dumpId: must not be 0: entry number %d", idx)
		}
		res = append(res, entry.DumpId)
		_, err = tmpListFile.Write([]byte(fmt.Sprintf("%d; \n", entry.DumpId)))
		if err != nil {
			return nil, fmt.Errorf("unable to write line into list file: %w", err)
		}
	}

	return res, nil
}

func removeEscapeQuotes(v string) string {
	if v[0] == '"' {
		v = v[1:]
	}
	if v[len(v)-1] == '"' {
		v = v[:len(v)-1]
	}
	return v
}
