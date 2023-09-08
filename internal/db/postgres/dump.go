package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	runtimeContext "github.com/GreenmaskIO/greenmask/internal/db/postgres/context"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/config"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/dump"
	storageDto "github.com/GreenmaskIO/greenmask/internal/db/postgres/domains/storage"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/dumpers"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/pgdump"
	"github.com/GreenmaskIO/greenmask/internal/db/postgres/toc"
	"github.com/GreenmaskIO/greenmask/internal/storage"
	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

type Dump struct {
	dsn                string
	pgDumpOptions      *pgdump.Options
	pgDump             *pgdump.PgDump
	dumpIdSequence     *toc.DumpIdSequence
	st                 storage.Storager
	dumpTaskCount      int32
	allTaskPushed      atomic.Bool
	config             []*config.Table
	dataEntryProducers []toc.EntryProducer
	context            *runtimeContext.RuntimeContext
	transformersMap    map[string]*toolkit.Definition
	schemaToc          *toc.Toc
	resultToc          *toc.Toc
}

func NewDump(binPath string, opt *pgdump.Options, st storage.Storager, cfg []*config.Table) *Dump {
	return &Dump{
		pgDumpOptions: opt,
		pgDump:        pgdump.NewPgDump(binPath),
		st:            st,
		config:        cfg,
	}
}

func (d *Dump) prune() {
	d.schemaToc = nil
	d.context = nil
	d.schemaToc = nil
	d.resultToc = nil
	d.transformersMap = nil
	d.dumpTaskCount = 0
	d.allTaskPushed.Store(false)
	d.dumpIdSequence = nil
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
		tx.Rollback(ctx)
		return nil, fmt.Errorf("cannot set transaction isolation level: %w", err)
	}
	rows.Close()

	if d.pgDumpOptions.Snapshot == "" {
		log.Debug().Msg("performing snapshot export")
		row := tx.QueryRow(ctx, "SELECT pg_export_snapshot()")
		if err := row.Scan(&d.pgDumpOptions.Snapshot); err != nil {
			tx.Rollback(ctx)
			return nil, fmt.Errorf("cannot export snapshot: %w", err)
		}
	} else {
		var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", d.pgDumpOptions.Snapshot)
		log.Debug().Msgf("performing %s snapshot import", d.pgDumpOptions.Snapshot)
		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			return nil, fmt.Errorf("cannot import snapshot: %w", err)
			tx.Rollback(ctx)
		}
	}

	return tx, nil
}

func (d *Dump) buildContextAndValidate(ctx context.Context, tx pgx.Tx) (err error) {
	d.context, err = runtimeContext.NewRuntimeContext(ctx, tx, d.config, d.transformersMap, d.pgDumpOptions)
	if err != nil {
		return fmt.Errorf("unable to build runtime context: %w", err)
	}
	// TODO: Implement warnings hook, such as logging and HTTP sender
	log.Warn().Msg("IMPLEMENT ME: warnings hook, such as logging and HTTP sender")
	for _, w := range d.context.Warnings {
		log.Debug().Any("ValidationWarning", w).Msg("")
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

	dumpDir := d.st.Getcwd()
	options.FileName = dumpDir
	if err := d.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	log.Debug().Msg("renaming toc file")
	if err := d.st.Rename(ctx, "toc.dat", "~toc.dat"); err != nil {
		return fmt.Errorf("cannot rename toc.dat file: %w", err)
	}

	log.Debug().Msg("reading schema section")
	srcTocFile, err := d.st.GetReader(ctx, "~toc.dat")
	if err != nil {
		return err
	}
	defer func() {
		srcTocFile.Close()
		if err := d.st.Delete(ctx, "~toc.dat", false); err != nil {
			log.Warn().Err(err).Msgf("unable to delete temp file")
		}
	}()
	rocReader := toc.NewReader(srcTocFile)
	schemaToc, err := rocReader.Read()
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}
	d.schemaToc = schemaToc
	d.dumpIdSequence = toc.NewDumpSequence(schemaToc.Header.MaxDumpId + 1)

	return nil
}

func (d *Dump) dataDump(ctx context.Context) error {
	// TODO: You should use pointer to dumpers.DumpTask instead
	tasks := make(chan dumpers.DumpTask, d.pgDumpOptions.Jobs)
	result := make(chan toc.EntryProducer, d.pgDumpOptions.Jobs)
	defer close(result)

	log.Debug().Msgf("planned %d workers", d.pgDumpOptions.Jobs)
	eg, gtx := errgroup.WithContext(ctx)
	for j := 0; j < d.pgDumpOptions.Jobs; j++ {
		eg.Go(func(id int) func() error {
			return func() error {
				return d.dumpWorker(gtx, tasks, result, id+1)
			}
		}(j))
	}

	// TODO: Implement LO dumping
	log.Warn().Msg("FIXME: implement Large Objects dumper")

	eg.Go(func() error {
		defer close(tasks)

		for _, dumpObj := range d.context.DataSectionObjects {
			log.Warn().Msg("implement data exclusion")
			dumpObj.SetDumpId(d.dumpIdSequence.Next())
			var task dumpers.DumpTask
			switch v := dumpObj.(type) {
			case *dump.Table:
				task = dumpers.NewTableDumper(v)
			case *dump.Sequence:
				task = dumpers.NewSequenceDumper(v)
			case *dump.LargeObject:
				return fmt.Errorf("is not implemented")
			default:
				return fmt.Errorf("unknow dumper type")
			}
			atomic.AddInt32(&d.dumpTaskCount, 1)
			select {
			case <-gtx.Done():
				return gtx.Err()
			case tasks <- task:
			}
		}
		d.allTaskPushed.Store(true)
		return nil
	})

	d.dataEntryProducers = make([]toc.EntryProducer, 0, len(d.context.DataSectionObjects))
	eg.Go(func() error {
		var tables, sequences, largeObjects []toc.EntryProducer
		for i := int32(0); !d.allTaskPushed.Load() || i < atomic.LoadInt32(&d.dumpTaskCount); i++ {
			select {
			case <-gtx.Done():
				return gtx.Err()
			case entry, ok := <-result:
				if entry == nil && ok {
					panic("unexpected entry nil pointer")
				}
				switch entry.(type) {
				case *dump.Table:
					tables = append(tables, entry)
				case *dump.Sequence:
					sequences = append(sequences, entry)
				case *dump.LargeObject:
					largeObjects = append(largeObjects, entry)
				default:
					return fmt.Errorf("unexpected toc entry type")
				}
			}
		}
		d.dataEntryProducers = append(d.dataEntryProducers, tables...)
		d.dataEntryProducers = append(d.dataEntryProducers, sequences...)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Debug().Msg("all the data have been dumped")
	return nil
}

func (d *Dump) mergeAndWriteToc(ctx context.Context, tx pgx.Tx) error {
	log.Debug().Msg("merging toc entries")
	mergedEntries, err := d.MergeTocEntries(d.schemaToc.Entries, d.dataEntryProducers)
	if err != nil {
		return fmt.Errorf("unable to mergeAndWriteToc TOC files: %w", err)
	}

	log.Debug().Msg("writing built toc file")
	destTocFile, err := d.st.GetWriter(ctx, "toc.dat")
	if err != nil {
		return err
	}
	defer destTocFile.Close()
	mergedHeader := *d.schemaToc.Header
	mergedHeader.TocCount = int32(len(mergedEntries))
	d.resultToc = &toc.Toc{
		Header:  &mergedHeader,
		Entries: mergedEntries,
	}
	tocWriter := toc.NewWriter(destTocFile)
	if err = tocWriter.Write(d.resultToc); err != nil {
		return fmt.Errorf("error writing toc file: %w", err)
	}
	return nil
}

func (d *Dump) writeMetaData(ctx context.Context, startedAt, completedAt time.Time) error {
	metadata, err := storageDto.NewMetadata(d.resultToc.Header, d.dataEntryProducers, 0, startedAt, completedAt, d.config)
	if err != nil {
		return fmt.Errorf("unable build metadata: %w", err)
	}
	meta, err := d.st.GetWriter(ctx, "metadata.json")
	if err != nil {
		return fmt.Errorf("unable to open metadata file: %w", err)
	}
	defer meta.Close()
	if err := json.NewEncoder(meta).Encode(metadata); err != nil {
		return fmt.Errorf("unable to write metadata: %w", err)
	}
	return nil
}

func (d *Dump) Run(ctx context.Context) (err error) {
	defer d.prune()
	startedAt := time.Now()

	d.transformersMap, err = runtimeContext.BuildTransformersMap()
	if err != nil {
		return fmt.Errorf("error building transformers map: %w", err)
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

	if err := d.buildContextAndValidate(ctx, tx); err != nil {
		return fmt.Errorf("context error: %w", err)
	}

	if d.pgDumpOptions.Validate {
		return nil
	}

	if err = d.schemaOnlyDump(ctx, tx); err != nil {
		return fmt.Errorf("schema only stage dumping error: %w", err)
	}

	if err = d.dataDump(ctx); err != nil {
		return fmt.Errorf("data stage dumping error: %w", err)
	}

	if err = d.mergeAndWriteToc(ctx, tx); err != nil {
		return fmt.Errorf("mergeAndWriteToc stage dumping error: %w", err)
	}

	if err = d.writeMetaData(ctx, startedAt, time.Now()); err != nil {
		return fmt.Errorf("writeMetaData stage dumping error: %w", err)
	}

	return nil
}

func (d *Dump) MergeTocEntries(schemaEntries []*toc.Entry, dataEntryProducers []toc.EntryProducer) ([]*toc.Entry, error) {
	// TODO: Assign dependencies and sort entries in the same order
	res := make([]*toc.Entry, 0, len(schemaEntries)+len(dataEntryProducers))

	preDataEnd := 0
	postDataStart := 0

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
	for _, ep := range dataEntryProducers {
		entry, err := ep.Entry()
		if err != nil {
			return nil, fmt.Errorf("cannot produce entry: %w", err)
		}
		res = append(res, entry)
	}
	res = append(res, schemaEntries[postDataStart:]...)

	return res, nil
}

func (d *Dump) dumpWorker(ctx context.Context, tasks <-chan dumpers.DumpTask, result chan<- toc.EntryProducer, id int) error {
	var isolationLevel = "REPEATABLE READ"
	if d.pgDumpOptions.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}
	var setIsolationLevelQuery = fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel)
	var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", d.pgDumpOptions.Snapshot)

	conn, err := pgx.Connect(ctx, d.dsn)
	if err != nil {
		return fmt.Errorf("cannot connecti to server (worker %d): %w", id, err)
	}
	defer conn.Close(ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cannot start transaction (worker %d): %w", id, err)
	}
	defer tx.Rollback(ctx)

	if !d.pgDumpOptions.NoSynchronizedSnapshots {
		if _, err := tx.Exec(ctx, setIsolationLevelQuery); err != nil {
			return fmt.Errorf("unable to set transaction isolation level (worker %d): %w", id, err)
		}

		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			return fmt.Errorf("cannot import snapshot (worker %d): %w", id, err)
		}
	}

	for {
		var task dumpers.DumpTask
		select {
		case <-ctx.Done():
			log.Debug().
				Err(ctx.Err()).
				Int("workerID", id).
				Str("objectName", task.DebugInfo()).
				Msgf("existed due to cancelled context")
			return ctx.Err()
		case task = <-tasks:
			if task == nil {
				log.Debug().
					Err(ctx.Err()).
					Int("workerID", id).
					Msgf("exited normally")
				return nil
			}
		}
		log.Debug().
			Int("workerID", id).
			Str("objectName", task.DebugInfo()).
			Msgf("dumping started")

		entry, err := task.Execute(ctx, tx, d.st)
		if err != nil {
			return err
		}
		if entry == nil {
			panic("received nil entry")
		}
		result <- entry
		log.Debug().
			Int("workerID", id).
			Str("objectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
