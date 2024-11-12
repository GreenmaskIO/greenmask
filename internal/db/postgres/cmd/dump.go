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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"slices"
	"time"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dumpers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	_ "github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	MetadataJsonFileName = "metadata.json"
	HeartBeatFileName    = "heartbeat"
)

const (
	HeartBeatWriteInterval = 15 * time.Minute
)

const (
	HeartBeatDoneContent       = "done"
	HeartBeatInProgressContent = "in-progress"
)

type Dump struct {
	dsn               string
	pgDumpOptions     *pgdump.Options
	pgDump            *pgdump.PgDump
	dumpIdSequence    *toc.DumpIdSequence
	st                storages.Storager
	tmpDir            string
	config            *domains.Config
	dataEntries       []*toc.Entry
	context           *runtimeContext.RuntimeContext
	registry          *utils.TransformerRegistry
	schemaToc         *toc.Toc
	resultToc         *toc.Toc
	dumpedObjectSizes map[int32]storageDto.ObjectSizeStat
	tableOidToDumpId  map[toolkit.Oid]int32
	tocFileSize       int64
	version           int
	blobs             *entries.Blobs
	// dumpDependenciesGraph - map of table DumpId to its dependencies DumpIds. Stores in meta and uses for restoration
	// coordination according to the topological order
	dumpDependenciesGraph map[int32][]int32
	// sortedTablesDumpIds - sorted tables dump ids in topological order
	sortedTablesDumpIds []int32
	// validate shows that dump worker must be in validation mode
	validate          bool
	validateRowsLimit uint64
}

func NewDump(cfg *domains.Config, st storages.Storager, registry *utils.TransformerRegistry) *Dump {

	return &Dump{
		pgDumpOptions:     &cfg.Dump.PgDumpOptions,
		pgDump:            pgdump.NewPgDump(cfg.Common.PgBinPath),
		st:                st,
		config:            cfg,
		tmpDir:            path.Join(cfg.Common.TempDirectory, fmt.Sprintf("%d", time.Now().UnixNano())),
		dumpedObjectSizes: map[int32]storageDto.ObjectSizeStat{},
		registry:          registry,
		tableOidToDumpId:  make(map[toolkit.Oid]int32),
	}
}

func (d *Dump) prune() {
	d.schemaToc = nil
	d.context = nil
	d.schemaToc = nil
	d.resultToc = nil
	d.registry = nil
	d.dumpIdSequence = nil
	if err := os.RemoveAll(d.tmpDir); err != nil {
		log.Debug().Err(err).Msg("error deleting temp dir")
	}
}

func (d *Dump) gatherPgFacts(ctx context.Context, tx pgx.Tx) error {
	getVersionQuery := `
		select 
		    setting::INT 
		from pg_settings 
		where name = 'server_version_num'
	`

	row := tx.QueryRow(ctx, getVersionQuery)
	if err := row.Scan(&d.version); err != nil {
		return fmt.Errorf("error getting pg version: %w", err)
	}
	return nil
}

func (d *Dump) connect(ctx context.Context, dsn string) (*pgx.Conn, error) {

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	pgxdecimal.Register(conn.TypeMap())

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, err
	}

	d.dsn = dsn
	return conn, nil
}

func (d *Dump) startMainTx(ctx context.Context, conn *pgx.Conn) (pgx.Tx, error) {
	var isolationLevel = "REPEATABLE READ"
	if d.pgDumpOptions.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel))
	if err != nil {
		if err := tx.Rollback(ctx); err != nil {
			log.Debug().Err(err).Msg("unable to rollback transaction")
		}
		return nil, fmt.Errorf("cannot set transaction isolation level: %w", err)
	}
	rows.Close()

	if d.pgDumpOptions.Snapshot == "" {
		log.Debug().Msg("performing snapshot export")
		row := tx.QueryRow(ctx, "SELECT pg_export_snapshot()")
		if err := row.Scan(&d.pgDumpOptions.Snapshot); err != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Debug().Err(err).Msg("unable to rollback transaction")
			}
			return nil, fmt.Errorf("cannot export snapshot: %w", err)
		}
	} else {
		var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", d.pgDumpOptions.Snapshot)
		log.Debug().Msgf("performing %s snapshot import", d.pgDumpOptions.Snapshot)
		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			if err := tx.Rollback(ctx); err != nil {
				log.Warn().Err(err).Msg("unable to rollback transaction")
			}
			return nil, fmt.Errorf("cannot import snapshot: %w", err)
		}
	}

	return tx, nil
}

func (d *Dump) buildContextAndValidate(ctx context.Context, tx pgx.Tx) (err error) {
	d.context, err = runtimeContext.NewRuntimeContext(
		ctx, tx, &d.config.Dump, d.registry,
		d.config.Dump.VirtualReferences, d.version,
	)
	if err != nil {
		return fmt.Errorf("unable to build runtime context: %w", err)
	}
	// TODO: Implement warnings hook, such as logging and HTTP sender
	for _, w := range d.context.Warnings {
		if w.Severity == "error" {
			log.Error().Any("ValidationWarning", w).Msg("")
		}
	}
	if d.context.IsFatal() {
		return fmt.Errorf("fatal validation error")
	}

	return nil
}

func (d *Dump) schemaOnlyDump(ctx context.Context, tx pgx.Tx) error {
	// Dump schema
	options := *d.pgDumpOptions
	options.Format = "d"
	options.SchemaOnly = true

	options.FileName = d.tmpDir
	if err := d.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	log.Debug().Msg("reading schema section")
	tocFile, err := os.Open(path.Join(d.tmpDir, "toc.dat"))
	if err != nil {
		return fmt.Errorf("error openning schema toc file: %w", err)
	}
	defer tocFile.Close()

	defer func() {
		// Deleting file after closing it
		if err := os.Remove(path.Join(d.tmpDir, "toc.dat")); err != nil {
			log.Warn().Err(err).Msgf("unable to delete temp file")
		}
	}()
	rocReader := toc.NewReader(tocFile)
	schemaToc, err := rocReader.Read()
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}
	d.schemaToc = schemaToc
	d.dumpIdSequence = toc.NewDumpSequence(schemaToc.Header.MaxDumpId + 1)

	return nil
}

// dumpWorkerPlanner - plans dump workers based on d.pgDumpOptions.Jobs
func (d *Dump) dumpWorkerPlanner(ctx context.Context, tasks <-chan dumpers.DumpTask, done chan struct{}) func() error {
	return func() error {
		defer func() {
			close(done)
		}()
		workerEg, gtx := errgroup.WithContext(ctx)
		for j := 0; j < d.pgDumpOptions.Jobs; j++ {
			workerEg.Go(
				d.dumpWorkerRunner(gtx, tasks, j),
			)
		}
		if err := workerEg.Wait(); err != nil {
			return err
		}
		return nil
	}
}

// dumpWorkerRunner - runs dumpWorker or validateDumpWorker depending on the mode
func (d *Dump) dumpWorkerRunner(
	ctx context.Context, tasks <-chan dumpers.DumpTask, jobId int,
) func() error {
	return func() error {
		if d.validate {
			return d.validateDumpWorker(ctx, tasks, jobId)
		} else {
			return d.dumpWorker(ctx, tasks, jobId)
		}
	}
}

// taskProducer - produces tasks for dumpWorker based on d.context.DataSectionObjects
func (d *Dump) taskProducer(ctx context.Context, tasks chan<- dumpers.DumpTask) func() error {
	return func() error {
		defer close(tasks)
		dataObjects := d.context.DataSectionObjects
		if d.validate {
			dataObjects = d.context.DataSectionObjectsToValidate
		}

		for _, dumpObj := range dataObjects {
			dumpObj.SetDumpId(d.dumpIdSequence)
			var task dumpers.DumpTask
			switch v := dumpObj.(type) {
			case *entries.Table:
				if v.RelKind == 'p' {
					continue
				}
				task = dumpers.NewTableDumper(v, d.validate, d.validateRowsLimit, d.pgDumpOptions.Pgzip)
			case *entries.Sequence:
				task = dumpers.NewSequenceDumper(v)
			case *entries.Blobs:
				d.blobs = v
				task = dumpers.NewLargeObjectDumper(v, d.pgDumpOptions.Pgzip)
			default:
				return fmt.Errorf("unknow dumper type")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case tasks <- task:
			}
		}
		return nil
	}
}

// createTocEntries - creates TOC entries based on d.context.DataSectionObjects
// they will be stored in tod.dat file
func (d *Dump) createTocEntries() error {
	var sequences, largeObjects, tablesEntry []*toc.Entry
	var tables []*entries.Table

	for _, obj := range d.context.DataSectionObjects {
		entry, err := obj.Entry()
		if err != nil {
			return fmt.Errorf("error producing toc entry: %w", err)
		}
		switch v := obj.(type) {
		case *entries.Table:
			d.tableOidToDumpId[v.Oid] = entry.DumpId
			d.dumpedObjectSizes[entry.DumpId] = storageDto.ObjectSizeStat{
				Original:   v.OriginalSize,
				Compressed: v.CompressedSize,
			}
			if v.RelKind != 'p' {
				// Do not create TOC entry for partitioned tables because they are not dumped. Only their partitions are
				// dumped
				tablesEntry = append(tablesEntry, entry)
			}
			tables = append(tables, v)
		case *entries.Sequence:
			sequences = append(sequences, entry)
		case *entries.Blobs:
			d.dumpedObjectSizes[entry.DumpId] = storageDto.ObjectSizeStat{
				Original:   v.OriginalSize,
				Compressed: v.CompressedSize,
			}
			largeObjects = append(largeObjects, entry)
		default:
			panic("unexpected entry type")
		}
	}

	d.setDumpDependenciesGraph(tables)

	d.dataEntries = append(d.dataEntries, tablesEntry...)
	d.dataEntries = append(d.dataEntries, sequences...)
	d.dataEntries = append(d.dataEntries, largeObjects...)
	return nil
}

// setDumpDependenciesGraph - sets dumpDependenciesGraph of entries using their dumpId and build topological order of
// dump ids
func (d *Dump) setDumpDependenciesGraph(tables []*entries.Table) {
	sortedOids, graph := d.context.Graph.GetSortedTablesAndDependenciesGraph()
	d.dumpDependenciesGraph = make(map[int32][]int32)
	for _, oid := range sortedOids {
		idx := slices.IndexFunc(tables, func(entry *entries.Table) bool {
			return entry.Oid == oid || entry.RootPtOid == oid
		})
		if idx == -1 {
			panic(fmt.Sprintf("table not found: oid=%d", oid))
		}
		t := tables[idx]
		// Create dependencies graph with DumpId sequence for easier restoration coordination
		d.dumpDependenciesGraph[t.DumpId] = []int32{}
		for _, depOid := range graph[oid] {
			// If dependency table is not in the tables slice, it is likely excluded
			if !slices.Contains(sortedOids, depOid) {
				continue
			}
			// Find dependency table in the tables slice by OID
			depIdx := slices.IndexFunc(tables, func(depTable *entries.Table) bool {
				return depTable.Oid == depOid
			})
			if depIdx == -1 {
				panic("table not found")
			}
			// Append dependency table DumpId to the current table dependencies
			d.dumpDependenciesGraph[t.DumpId] = append(d.dumpDependenciesGraph[t.DumpId], tables[depIdx].DumpId)
		}
		d.sortedTablesDumpIds = append(d.sortedTablesDumpIds, t.DumpId)
	}
}

func (d *Dump) dataDump(ctx context.Context) error {
	tasks := make(chan dumpers.DumpTask, d.pgDumpOptions.Jobs)

	log.Debug().Msgf("planned %d workers", d.pgDumpOptions.Jobs)
	done := make(chan struct{})
	eg, gtx := errgroup.WithContext(ctx)
	eg.Go(d.writeHeartBeatWorker(gtx, done))
	eg.Go(d.dumpWorkerPlanner(gtx, tasks, done))
	eg.Go(d.taskProducer(gtx, tasks))

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	if err := d.createTocEntries(); err != nil {
		return fmt.Errorf("error creating toc entries: %w", err)
	}
	log.Debug().Msg("all the data have been dumped")
	return nil
}

func (d *Dump) mergeAndWriteToc(ctx context.Context) error {
	log.Debug().Msg("merging toc entries")
	mergedEntries, err := d.MergeTocEntries(d.schemaToc.Entries, d.dataEntries)
	if err != nil {
		return fmt.Errorf("unable to mergeAndWriteToc TOC files: %w", err)
	}

	log.Debug().Msg("writing built toc file into storage")
	// Create TOC
	mergedHeader := *d.schemaToc.Header
	mergedHeader.TocCount = int32(len(mergedEntries))
	d.resultToc = &toc.Toc{
		Header:  &mergedHeader,
		Entries: mergedEntries,
	}

	// Creating toc buffer for transferring to the storage
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	// Write toc to the buf
	if err = toc.NewWriter(buf).Write(d.resultToc); err != nil {
		return fmt.Errorf("error writing built toc file to the storage: %w", err)
	}
	d.tocFileSize = int64(buf.Len())
	// Writing dumped TOC into buffer to the storage
	if err = d.st.PutObject(ctx, "toc.dat", buf); err != nil {
		return err
	}

	return nil
}

func (d *Dump) writeMetaData(ctx context.Context, startedAt, completedAt time.Time) error {
	cycles := d.context.Graph.GetCycledTables()
	metadata, err := storageDto.NewMetadata(
		d.resultToc, d.tocFileSize, startedAt, completedAt, d.config.Dump.Transformation, d.dumpedObjectSizes,
		d.context.DatabaseSchema, d.dumpDependenciesGraph, d.sortedTablesDumpIds, cycles, d.tableOidToDumpId,
	)
	if err != nil {
		return fmt.Errorf("unable build metadata: %w", err)
	}

	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err = json.NewEncoder(buf).Encode(metadata); err != nil {
		return fmt.Errorf("error encoding metadata.json: %w", err)
	}

	if err = d.st.PutObject(ctx, MetadataJsonFileName, buf); err != nil {
		return fmt.Errorf("error writing metadata to the storage: %w", err)
	}
	return nil
}

func (d *Dump) Run(ctx context.Context) (err error) {
	defer d.prune()
	startedAt := time.Now()

	if err := custom.BootstrapCustomTransformers(ctx, d.registry, d.config.CustomTransformers); err != nil {
		return fmt.Errorf("error bootstraping custom transformers: %w", err)
	}

	dsn, err := d.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := d.connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tx, err := d.startMainTx(ctx, conn)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	if err = d.gatherPgFacts(ctx, tx); err != nil {
		return fmt.Errorf("error gathering facts: %w", err)
	}

	if err := d.buildContextAndValidate(ctx, tx); err != nil {
		return fmt.Errorf("context error: %w", err)
	}

	if err = d.schemaOnlyDump(ctx, tx); err != nil {
		return fmt.Errorf("schema only stage dumping error: %w", err)
	}

	if err = d.dataDump(ctx); err != nil {
		return fmt.Errorf("data stage dumping error: %w", err)
	}

	if err = d.mergeAndWriteToc(ctx); err != nil {
		return fmt.Errorf("mergeAndWriteToc stage dumping error: %w", err)
	}

	if err = d.writeMetaData(ctx, startedAt, time.Now()); err != nil {
		return fmt.Errorf("writeMetaData stage dumping error: %w", err)
	}

	return nil
}

func (d *Dump) MergeTocEntries(schemaEntries []*toc.Entry, dataEntries []*toc.Entry) (
	[]*toc.Entry, error,
) {
	if len(dataEntries) == 0 {
		// No data entries, just return schema entries
		return schemaEntries, nil
	}

	// TODO: Assign dependencies and sort entries in the same order
	res := make([]*toc.Entry, 0, len(schemaEntries)+len(dataEntries))

	preDataEnd := 0
	postDataStart := len(schemaEntries) - 1

	// Find predata last index and postdata first index
	for idx, item := range schemaEntries {
		if item.Section == toc.SectionPreData {
			preDataEnd = idx
		}
		if item.Section == toc.SectionPostData {
			postDataStart = idx
			break
		}
	}

	res = append(res, schemaEntries[:preDataEnd+1]...)
	if d.blobs != nil {
		blobsDDLEntries := d.blobs.GetAllDDLs()
		res = append(res, blobsDDLEntries...)
	}
	res = append(res, dataEntries...)
	res = append(res, schemaEntries[postDataStart:]...)

	return res, nil
}

func (d *Dump) getWorkerTransaction(ctx context.Context) (*pgx.Conn, pgx.Tx, error) {
	var isolationLevel = "REPEATABLE READ"
	if d.pgDumpOptions.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}
	var setIsolationLevelQuery = fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel)
	var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", d.pgDumpOptions.Snapshot)

	conn, err := pgx.Connect(ctx, d.dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot connecti to server: %w", err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		conn.Close(ctx)
		return nil, nil, fmt.Errorf("cannot start transaction: %w", err)
	}
	if !d.pgDumpOptions.NoSynchronizedSnapshots {
		if _, err := tx.Exec(ctx, setIsolationLevelQuery); err != nil {
			conn.Close(ctx)
			return nil, nil, fmt.Errorf("unable to set transaction isolation level: %w", err)
		}

		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			conn.Close(ctx)
			return nil, nil, fmt.Errorf("cannot import snapshot: %w", err)
		}
	}
	return conn, tx, nil
}

func (d *Dump) dumpWorker(
	ctx context.Context, tasks <-chan dumpers.DumpTask, id int,
) error {

	conn, tx, err := d.getWorkerTransaction(ctx)

	if err != nil {
		return fmt.Errorf("error preparing worker (id=%d) transaction: %w", id, err)
	}

	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Debug().Err(err).Int("WorkerId", id).Msg("error closing connection")
		}
	}()

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Debug().Err(err).Int("WorkerId", id).Msg("unable to rollback transaction")
		}
	}()

	for {

		var task dumpers.DumpTask
		var ok bool
		select {
		case <-ctx.Done():
			log.Debug().
				Err(ctx.Err()).
				Int("WorkerId", id).
				Msgf("existed due to cancelled context")
			return ctx.Err()
		case task, ok = <-tasks:
			if !ok {
				log.Debug().
					Err(ctx.Err()).
					Int("WorkerId", id).
					Msgf("exited normally")
				return nil
			}
		}
		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping started")

		if err = task.Execute(ctx, tx, d.st); err != nil {
			return err
		}

		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}

func (d *Dump) validateDumpWorker(
	ctx context.Context, tasks <-chan dumpers.DumpTask, id int,
) error {
	for {

		var task dumpers.DumpTask
		var ok bool
		select {
		case <-ctx.Done():
			log.Debug().
				Err(ctx.Err()).
				Int("WorkerId", id).
				Msgf("existed due to cancelled context")
			return ctx.Err()
		case task, ok = <-tasks:
			if !ok {
				log.Debug().
					Err(ctx.Err()).
					Int("WorkerId", id).
					Msgf("exited normally")
				return nil
			}
		}
		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping started")

		if err := d.validateDumpExecuteTask(ctx, id, task); err != nil {
			return err
		}

		log.Debug().
			Int("WorkerId", id).
			Str("ObjectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}

func (d *Dump) validateDumpExecuteTask(ctx context.Context, id int, task dumpers.DumpTask) error {
	// We do not need to manage transaction in case of validation - we just close the connection. According to the
	// documentation, the COPY stream can be interrupted by the client via connection close.
	// If you try to roll back the transaction we will face the deadlock.
	conn, tx, err := d.getWorkerTransaction(ctx)

	if err != nil {
		return fmt.Errorf("error preparing worker (id=%v) transaction: %w", id, err)
	}

	defer func() {
		if err := conn.Close(ctx); err != nil {
			log.Debug().Err(err).Int("WorkerId", id).Msg("error closing connection")
		}
	}()

	if err = task.Execute(ctx, tx, d.st); err != nil {
		return err
	}
	return nil
}

// writeHeartBeatWorker - writes heart beat file each HeartBeatWriteInterval and on the jobs completion
func (d *Dump) writeHeartBeatWorker(ctx context.Context, done chan struct{}) func() error {
	return func() error {
		// Initial write
		if err := d.writeHeartBeat(ctx, HeartBeatInProgressContent); err != nil {
			return fmt.Errorf("error writing heartbeat: %w", err)
		}
		t := time.NewTicker(HeartBeatWriteInterval)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-done:
				if err := d.writeHeartBeat(ctx, HeartBeatDoneContent); err != nil {
					return fmt.Errorf("error writing heartbeat: %w", err)
				}
				return nil
			case <-t.C:
				if err := d.writeHeartBeat(ctx, HeartBeatInProgressContent); err != nil {
					return fmt.Errorf("error writing heartbeat: %w", err)
				}
			}
		}
	}
}

// writeHeartBeat - write data in heart beat file
func (d *Dump) writeHeartBeat(ctx context.Context, data string) error {
	b := bytes.NewBuffer([]byte(data))
	if err := d.st.PutObject(ctx, HeartBeatFileName, b); err != nil {
		return err
	}
	return nil
}
