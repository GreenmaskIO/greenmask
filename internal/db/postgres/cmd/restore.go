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
	"os/exec"
	"path"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v3"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/restorers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/db/postgres/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/filestore"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
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

const dependenciesCheckInterval = 15 * time.Millisecond

var ErrTableDefinitionIsEmtpy = errors.New("table definition is empty: please re-dump the data using the latest version of greenmask if you want to use --inserts")

type restorationTask interface {
	Execute(ctx context.Context, conn utils.PGConnector) error
	DebugInfo() string
	GetEntry() *toc.Entry
}

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
	cfg        *domains.Restore
	metadata   *storage.Metadata
	mx         *sync.RWMutex

	preDataClenUpToc  string
	postDataClenUpToc string
	restoredDumpIds   map[int32]bool
}

func NewRestore(
	binPath string, st storages.Storager, cfg *domains.Restore, s map[string][]pgrestore.Script, tmpDir string,
) *Restore {
	return &Restore{
		binPath:         binPath,
		st:              st,
		pgRestore:       pgrestore.NewPgRestore(binPath),
		restoreOpt:      &cfg.PgRestoreOptions,
		scripts:         s,
		tmpDir:          path.Join(tmpDir, fmt.Sprintf("%d", time.Now().UnixNano())),
		cfg:             cfg,
		metadata:        &storage.Metadata{},
		restoredDumpIds: make(map[int32]bool),
		mx:              &sync.RWMutex{},
	}
}

func (r *Restore) Run(ctx context.Context) error {
	defer r.prune()

	if err := r.readMetadata(ctx); err != nil {
		return fmt.Errorf("cannot read metadata: %w", err)
	}

	if err := r.prepare(); err != nil {
		return fmt.Errorf("preparation error: %w", err)
	}

	if err := r.preFlightRestore(ctx); err != nil {
		return fmt.Errorf("pre-flight stage restoration error: %w", err)
	}

	if err := r.preDataRestore(ctx); err != nil {
		return fmt.Errorf("pre-data stage restoration error: %w", err)
	}

	if err := r.dataRestore(ctx); err != nil {
		return fmt.Errorf("data stage restoration error: %w", err)
	}

	if err := r.postDataRestore(ctx); err != nil {
		return fmt.Errorf("post-data stage restoration error: %w", err)
	}

	if err := filestore.Restore(ctx, r.cfg.Filestore, r.st); err != nil {
		return fmt.Errorf("filestore restoration error: %w", err)
	}

	return nil
}

func (r *Restore) putDumpId(task restorationTask) {
	r.mx.Lock()
	r.restoredDumpIds[task.GetEntry().DumpId] = true
	r.mx.Unlock()
}

func (r *Restore) dependenciesAreRestored(deps []int32) bool {
	r.mx.RLock()
	defer r.mx.RUnlock()
	for _, id := range deps {
		if !r.restoredDumpIds[id] {
			return false
		}
	}
	return true
}

func (r *Restore) readMetadata(ctx context.Context) error {
	f, err := r.st.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return fmt.Errorf("cannot open metadata file: %w", err)
	}
	if err := json.NewDecoder(f).Decode(r.metadata); err != nil {
		return fmt.Errorf("cannot decode metadata: %w", err)
	}
	return nil
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

func (r *Restore) preFlightRestore(ctx context.Context) error {
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

func (r *Restore) preDataRestore(ctx context.Context) error {
	// pg_dump has a limitation:
	//	If we want to use --cleanup command then this command must be performed for whole schema (--schema-only)
	//  without --section parameter. For avoiding cascade dropping we need to run pg_restore with --schema-only --clean
	//  and then remove all post-data objects manually. If we call pg_restore with --section=pre-data --clean then it
	//  causes errors because we need to drop post-data dependencies before dropping pre-data
	//
	//	In current implementation Greenmask modifies toc.dat file by removing create statement in post-data section
	//  and applies this toc.dat in the pre-data section restoration. The post-data restoration uses original
	//  (non modified) toc.dat file.

	// Do not restore this section if implicitly provided another section
	if r.restoreOpt.DataOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != preDataSection) {
		return nil
	}

	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot establish connection to db: %w", err)
	}
	defer conn.Close(ctx)

	// Execute PreData Before scripts
	if err := r.RunScripts(ctx, conn, scriptPreDataSection, scriptExecuteBefore); err != nil {
		return err
	}

	options := *r.restoreOpt

	if r.restoreOpt.Clean && r.restoreOpt.Section == "" {
		// Handling parameters for --clean
		options.SchemaOnly = true

		// Build clean up toc for dropping dependant objects in post-data stage without restoration them
		// right now
		var err error
		r.preDataClenUpToc, r.postDataClenUpToc, err = r.prepareCleanupToc()
		if err != nil {
			return fmt.Errorf("cannot prepare clean up toc: %w", err)
		}
		options.DirPath = r.preDataClenUpToc
	} else {
		options.DirPath = r.tmpDir
		options.Section = "pre-data"
	}

	if err := r.pgRestore.Run(ctx, &options); err != nil {
		var exitErr *exec.ExitError
		if r.restoreOpt.ExitOnError || (errors.As(err, &exitErr) && exitErr.ExitCode() != 1) {
			return fmt.Errorf("cannot restore pre-data section using pg_restore: %w", err)
		}
	}

	// Execute PreData After scripts
	if err := r.RunScripts(ctx, conn, scriptPreDataSection, scriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

// prepareCleanupToc - replaces create statements in post-data section with SELECT 1 and stores in tmp directory
func (r *Restore) prepareCleanupToc() (string, string, error) {
	preDataCleanUpToc := r.tocObj.Copy()
	postDataCleanUpToc := r.tocObj.Copy()

	statementReplacements := ";"

	for idx := range preDataCleanUpToc.Entries {
		log.Debug().Int("a", idx)
		preEntry := preDataCleanUpToc.Entries[idx]
		if preEntry.Section == toc.SectionPostData && preEntry.Defn != nil {
			preEntry.Defn = &statementReplacements
		}

		postEntry := postDataCleanUpToc.Entries[idx]
		if postEntry.Section == toc.SectionPostData && postEntry.DropStmt != nil {
			postEntry.DropStmt = &statementReplacements
		}
	}

	preDatadirName := path.Join(r.tmpDir, "pre_data_clean_up_toc")
	postDatadirName := path.Join(r.tmpDir, "post_data_clean_up_toc")

	// Pre-data section

	if err := os.Mkdir(preDatadirName, 0700); err != nil {
		return "", "", fmt.Errorf("cannot create pre-data clean up toc directory: %w", err)
	}

	f1, err := os.Create(path.Join(preDatadirName, "toc.dat"))
	if err != nil {
		return "", "", fmt.Errorf("cannot create clean up toc file: %w", err)
	}
	defer f1.Close()

	if err = toc.NewWriter(f1).Write(preDataCleanUpToc); err != nil {
		return "", "", fmt.Errorf("cannot write clean up toc: %w", err)
	}

	// post-data section
	if err = os.Mkdir(postDatadirName, 0700); err != nil {
		return "", "", fmt.Errorf("cannot create post-data clean up toc directory: %w", err)
	}

	f2, err := os.Create(path.Join(postDatadirName, "toc.dat"))
	if err != nil {
		return "", "", fmt.Errorf("cannot create clean up toc file: %w", err)
	}
	defer f2.Close()

	if err = toc.NewWriter(f2).Write(postDataCleanUpToc); err != nil {
		return "", "", fmt.Errorf("cannot write clean up toc: %w", err)
	}

	return preDatadirName, postDatadirName, nil
}

func (r *Restore) dataRestore(ctx context.Context) error {
	// Execute Data Before scripts

	// Do not restore this section if implicitly provided
	if r.restoreOpt.SchemaOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != dataSection) {
		return nil
	}

	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot establish connection to db: %w", err)
	}
	defer conn.Close(ctx)

	if err = r.RunScripts(ctx, conn, scriptDataSection, scriptExecuteBefore); err != nil {
		return err
	}

	tasks := make(chan restorationTask, r.restoreOpt.Jobs)
	eg, gtx := errgroup.WithContext(ctx)

	for j := range r.restoreOpt.Jobs {
		eg.Go(func(id int) func() error {
			return func() error {
				return r.restoreWorker(gtx, tasks, id+1)
			}
		}(j))
	}

	eg.Go(r.taskPusher(gtx, tasks))

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

			return false
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

func (r *Restore) postDataRestore(ctx context.Context) error {
	// Execute Post Data Before scripts

	// Do not restore this section if implicitly provided
	if r.restoreOpt.DataOnly ||
		(r.restoreOpt.Section != "" && r.restoreOpt.Section != postDataSection) {
		return nil
	}

	conn, err := pgx.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("cannot establish connection to db: %w", err)
	}
	defer conn.Close(ctx)

	if err = r.RunScripts(ctx, conn, scriptPostDataSection, scriptExecuteBefore); err != nil {
		return err
	}

	options := *r.restoreOpt
	options.Section = "post-data"
	options.DirPath = r.tmpDir

	if r.postDataClenUpToc != "" {
		options.DirPath = r.postDataClenUpToc
	}

	if err = r.pgRestore.Run(ctx, &options); err != nil {
		var exitErr *exec.ExitError
		if r.restoreOpt.ExitOnError || (errors.As(err, &exitErr) && exitErr.ExitCode() != 1) {
			return fmt.Errorf("cannot restore post-data section using pg_restore: %w", err)
		}
	}

	if err = r.RunScripts(ctx, conn, scriptPostDataSection, scriptExecuteAfter); err != nil {
		return err
	}

	return nil
}

func (r *Restore) prune() {
	if err := os.RemoveAll(r.tmpDir); err != nil {
		log.Debug().Err(err).Msg("error deleting temp dir")
	}
}

func (r *Restore) logWarningsIfHasCycles() {
	if len(r.metadata.Cycles) == 0 {
		return
	}
	for _, cycle := range r.metadata.Cycles {
		log.Warn().
			Strs("cycle", cycle).
			Msg("cycle between tables is detected: cannot guarantee the order of restoration within cycle")
	}
}

func (r *Restore) sortTocEntriesInTopoOrder(entries []*toc.Entry) []*toc.Entry {
	r.logWarningsIfHasCycles()

	// Find data section entries - tables are sorted topologically
	sortedTablesEntries := make([]*toc.Entry, 0, len(entries))
	for _, dumpId := range r.metadata.DumpIdsOrder {
		idx := slices.IndexFunc(entries, func(entry *toc.Entry) bool {
			return entry.DumpId == dumpId
		})
		if idx == -1 {
			tableOid, ok := r.metadata.DumpIdsToTableOid[dumpId]
			if !ok {
				panic(fmt.Sprintf("table with dumpId %d is not found in dumpId to Oids map", dumpId))
			}
			skippedTableIdx := slices.IndexFunc(r.metadata.DatabaseSchema, func(t *toolkit.Table) bool {
				return t.Oid == tableOid
			})
			if skippedTableIdx == -1 {
				panic(fmt.Sprintf("table with oid %d is not found in DatabaseSchema", tableOid))
			}
			log.Debug().
				Int32("DumpId", dumpId).
				Str("SchemaName", r.metadata.DatabaseSchema[skippedTableIdx].Schema).
				Str("TableName", r.metadata.DatabaseSchema[skippedTableIdx].Name).
				Msg("table might be excluded from dump or it is a partitioned table (not partition itself): table is not found in dump entries")
			continue
		}
		sortedTablesEntries = append(sortedTablesEntries, entries[idx])
	}

	// Add non-table DATA section entries (like SEQUENCE SET, BLOBS, etc.)
	// These entries are not part of the dependency graph but must still be restored

	// Build a map of already added DumpIds for O(1) duplicate checking
	seenDumpIds := make(map[int32]bool, len(sortedTablesEntries))
	for _, entry := range sortedTablesEntries {
		seenDumpIds[entry.DumpId] = true
	}

	// Add non-table entries that haven't been added yet
	// Note: Not prefiltering entries as performance gain is negligible for typical dataset sizes (KISS)
	for _, entry := range entries {
		if entry.Desc != nil && *entry.Desc != toc.TableDataDesc {
			if !seenDumpIds[entry.DumpId] {
				sortedTablesEntries = append(sortedTablesEntries, entry)
			}
		}
	}

	return sortedTablesEntries
}

func (r *Restore) waitDependenciesAreRestore(ctx context.Context, deps []int32) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if r.dependenciesAreRestored(deps) {
			return nil
		}
		time.Sleep(dependenciesCheckInterval)
	}
}

func (r *Restore) taskPusher(ctx context.Context, tasks chan restorationTask) func() error {
	return func() error {
		defer close(tasks)
		tocEntries := getDataSectionTocEntries(r.tocObj.Entries)
		if r.restoreOpt.RestoreInOrder {
			tocEntries = r.sortTocEntriesInTopoOrder(tocEntries)
		}
		for _, entry := range tocEntries {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			if entry.Section == toc.SectionData {

				if !r.isNeedRestore(entry) {
					continue
				}

				if r.restoreOpt.RestoreInOrder && r.restoreOpt.Jobs > 1 {
					deps := r.metadata.DependenciesGraph[entry.DumpId]
					if err := r.waitDependenciesAreRestore(ctx, deps); err != nil {
						return fmt.Errorf("cannot wait for dependencies are restored: %w", err)
					}
				}

				var task restorationTask
				switch *entry.Desc {
				case toc.TableDataDesc:
					if r.restoreOpt.Inserts || r.restoreOpt.OnConflictDoNothing {
						t, err := r.getTableDefinitionFromMeta(entry.DumpId)
						if err != nil {
							return fmt.Errorf("cannot get table definition from meta: %w", err)
						}
						task = restorers.NewTableRestorerInsertFormat(
							entry, t, r.st, r.restoreOpt.ToDataSectionSettings(), r.cfg.ErrorExclusions,
						)
					} else {
						task = restorers.NewTableRestorer(entry, r.st, r.restoreOpt.ToDataSectionSettings())
					}

				case toc.SequenceSetDesc:
					task = restorers.NewSequenceRestorer(entry)
				case toc.BlobsDesc:
					if r.restoreOpt.NoBlobs {
						// Skip blobs restoration
						log.Debug().
							Int32("DumpId", entry.DumpId).
							Msg("blobs restoration is skipped")
						continue
					}
					task = restorers.NewBlobsRestorer(entry, r.st, r.restoreOpt.Pgzip)
				}

				if task != nil {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case tasks <- task:
					}
				}
			}

		}
		return nil
	}
}

func (r *Restore) getTableDefinitionFromMeta(dumpId int32) (*toolkit.Table, error) {
	tableOid, ok := r.metadata.DumpIdsToTableOid[dumpId]
	if !ok {
		return nil, ErrTableDefinitionIsEmtpy
	}
	idx := slices.IndexFunc(r.metadata.DatabaseSchema, func(t *toolkit.Table) bool {
		return t.Oid == tableOid
	})
	if idx == -1 {
		panic(fmt.Sprintf("table with oid %d is not found in metadata", tableOid))
	}
	return r.metadata.DatabaseSchema[idx], nil
}

func (r *Restore) restoreWorker(ctx context.Context, tasks <-chan restorationTask, id int) error {
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
		var task restorationTask
		select {
		case <-ctx.Done():
			log.Debug().
				Int("workerId", id).
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
		if err = task.Execute(ctx, utils.NewPGConn(conn)); err != nil {
			return fmt.Errorf("unable to perform restoration task (worker %d restoring %s): %w", id, task.DebugInfo(), err)
		}
		r.putDumpId(task)
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
	lineBuf := make([]byte, 0, 1024)
	buf := bytes.NewBuffer(lineBuf)
	lr := bufio.NewReader(f)
	pattern, err := regexp.Compile(`^\s*(?P<DumpIdSequence>\d+)\s*;.*$`)
	if err != nil {
		return nil, fmt.Errorf("cannot compile regexp: %s", err)
	}
	// res := make(map[int32]bool)
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

func getDataSectionTocEntries(tocEntries []*toc.Entry) []*toc.Entry {
	var dataSectionEntries []*toc.Entry
	for _, entry := range tocEntries {
		if entry.Section == toc.SectionData {
			dataSectionEntries = append(dataSectionEntries, entry)
		}
	}
	return dataSectionEntries
}
