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

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/dumpers"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
)

type Dump struct {
	dsn            string
	pgDumpOptions  *pgdump.Options
	pgDump         *pgdump.PgDump
	curDumpId      domains.DumpId
	st             storage.Storager
	dumpTaskCount  int32
	allTaskPushed  atomic.Bool
	tableConfig    []domains.Table
	ah             *toc.ArchiveHandle
	tocDataEntries []*toc.Entry
}

func NewDump(binPath string, opt *pgdump.Options, st storage.Storager, tableConfig []domains.Table) *Dump {
	return &Dump{
		pgDumpOptions: opt,
		pgDump:        pgdump.NewPgDump(binPath),
		st:            st,
		tableConfig:   tableConfig,
	}
}

func (d *Dump) Connect(ctx context.Context, dsn string) (*pgx.Conn, error) {

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
	schemaToc, err := toc.ReadFile(srcTocFile)
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}
	d.ah = schemaToc
	d.curDumpId = domains.DumpId(schemaToc.MaxDumpId + 1)

	return nil
}

func (d *Dump) dataDump(ctx context.Context, tx pgx.Tx) error {
	// TODO: You should use pointer to dumpers.DumpTask instead
	var largeObjectsList []*domains.LargeObjects
	tablesList, sequenceList, err := buildObjects(ctx, tx, d.pgDumpOptions, d.tableConfig, &d.curDumpId)
	if err != nil {
		return fmt.Errorf("building data objects: %w", err)
	}

	tasks := make(chan dumpers.DumpTask, d.pgDumpOptions.Jobs)
	result := make(chan *toc.Entry, d.pgDumpOptions.Jobs)
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
		for _, table := range tablesList {
			if table.ExcludeData {
				continue
			}
			atomic.AddInt32(&d.dumpTaskCount, 1)
			select {
			case <-gtx.Done():
				return gtx.Err()
			case tasks <- dumpers.NewTableDumper(*table):
			}
		}
		for idx, sequence := range sequenceList {
			if sequence.ExcludeData {
				continue
			}

			// Once all task has been pushed we assign true value for allTaskPushed before writing into
			// the channel
			atomic.AddInt32(&d.dumpTaskCount, 1)
			if idx == len(sequenceList)-1 {
				d.allTaskPushed.Store(true)
			}

			select {
			case <-gtx.Done():
				return gtx.Err()
			case tasks <- dumpers.NewSequenceDumper(*sequence):
			}
		}
		return nil
	})

	var originalSize, compressedSize int64
	d.tocDataEntries = make([]*toc.Entry, 0, len(tablesList)+len(sequenceList)+len(largeObjectsList))
	eg.Go(func() error {
		tables := make([]*toc.Entry, 0, len(tablesList))
		sequences := make([]*toc.Entry, 0, len(sequenceList))
		largeObjects := make([]*toc.Entry, 0, 0)
		for i := int32(0); !d.allTaskPushed.Load() || i < atomic.LoadInt32(&d.dumpTaskCount); i++ {
			select {
			case <-gtx.Done():
				return gtx.Err()
			case tocEntry := <-result:
				switch *tocEntry.Desc {
				case domains.TableDataDesc:
					tables = append(tables, tocEntry)
				case domains.SequenceSetDesc:
					sequences = append(sequences, tocEntry)
				case domains.LargeObjectDesc:
					largeObjects = append(largeObjects, tocEntry)
				default:
					return fmt.Errorf("unexpected toc entry %s", *tocEntry.Desc)
				}
				originalSize += tocEntry.OriginalSize
				compressedSize += tocEntry.CompressedSize
			}
		}
		d.tocDataEntries = append(d.tocDataEntries, tables...)
		d.tocDataEntries = append(d.tocDataEntries, sequences...)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	return nil
}

func (d *Dump) merge(ctx context.Context, tx pgx.Tx) error {
	log.Debug().Msg("merging toc entries")
	mergedTocs, err := d.MergeTocEntries(d.ah.GetEntries(), d.tocDataEntries)
	if err != nil {
		return fmt.Errorf("unable to merge TOC files: %w", err)
	}

	log.Debug().Msg("writing built toc file")
	destTocFile, err := d.st.GetWriter(ctx, "toc.dat")
	if err != nil {
		return err
	}
	defer destTocFile.Close()
	d.ah.SetEntries(mergedTocs)
	if err = toc.WriteFile(d.ah, destTocFile); err != nil {
		return fmt.Errorf("error writing toc file: %w", err)
	}
	return nil
}

func (d *Dump) writeMetaData(ctx context.Context, startedAt, completedAt time.Time) error {
	metadata, err := domains.NewMetadata(d.ah.Header, d.ah.GetEntries(),
		d.ah.WrittenBytes, startedAt, completedAt,
		d.tableConfig,
	)
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

func (d *Dump) RunDump(ctx context.Context) error {

	startedAt := time.Now()

	dsn, err := d.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := d.Connect(ctx, dsn)
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

	if err = d.schemaOnlyDump(ctx, tx); err != nil {
		return fmt.Errorf("schema only stage dumping error: %w", err)
	}

	if err = d.dataDump(ctx, tx); err != nil {
		return fmt.Errorf("data stage dumping error: %w", err)
	}

	if err = d.merge(ctx, tx); err != nil {
		return fmt.Errorf("merge stage dumping error: %w", err)
	}

	if err = d.writeMetaData(ctx, startedAt, time.Now()); err != nil {
		return fmt.Errorf("writeMetaData stage dumping error: %w", err)
	}

	return nil
}

func (d *Dump) MergeTocEntries(schema, data []*toc.Entry) ([]*toc.Entry, error) {
	// TODO: Assign dependencies and sort entries in the same order
	res := make([]*toc.Entry, 0, len(schema)+len(data))

	preDataEnd := 0
	postDataStart := 0

	// Find predata last index and postdata first index
	for idx, item := range schema {
		if item.Section == toc.SectionPreData {
			preDataEnd = idx
		}
		if item.Section == toc.SectionPostData {
			postDataStart = idx
			break
		}
	}

	res = append(res, schema[:preDataEnd+1]...)
	res = append(res, data...)
	res = append(res, schema[postDataStart:]...)

	return res, nil
}

func (d *Dump) dumpWorker(ctx context.Context, tasks <-chan dumpers.DumpTask, result chan<- *toc.Entry, id int) error {
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
		result <- entry
		log.Debug().
			Int("workerID", id).
			Str("objectName", task.DebugInfo()).
			Msgf("dumping is done")
	}
}
