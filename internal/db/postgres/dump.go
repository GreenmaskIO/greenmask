package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"time"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	runtimeContext "github.com/greenmaskio/greenmask/internal/db/postgres/context"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/dumpers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	storageDto "github.com/greenmaskio/greenmask/internal/db/postgres/storage"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	_ "github.com/greenmaskio/greenmask/internal/db/postgres/transformers"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/custom"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

const MetadataJsonFileName = "metadata.json"

type Dump struct {
	dsn               string
	pgDumpOptions     *pgdump.Options
	pgDump            *pgdump.PgDump
	dumpIdSequence    *toc.DumpIdSequence
	st                storages.Storager
	tmpDir            string
	dumpTaskCount     int32
	allTaskPushed     atomic.Bool
	config            *domains.Config
	dataEntries       []*toc.Entry
	context           *runtimeContext.RuntimeContext
	registry          *utils.TransformerRegistry
	schemaToc         *toc.Toc
	resultToc         *toc.Toc
	dumpedObjectSizes map[int32]storageDto.ObjectSizeStat
	tocFileSize       int64
	version           int
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
	}
}

func (d *Dump) prune() {
	d.schemaToc = nil
	d.context = nil
	d.schemaToc = nil
	d.resultToc = nil
	d.registry = nil
	d.dumpTaskCount = 0
	d.allTaskPushed.Store(false)
	d.dumpIdSequence = nil
	if err := os.RemoveAll(d.tmpDir); err != nil {
		log.Debug().Err(err).Msg("error deleting temp dir")
	}
	clear(d.dumpedObjectSizes)
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
	d.context, err = runtimeContext.NewRuntimeContext(ctx, tx, d.config.Dump.Transformation, d.registry,
		d.pgDumpOptions, d.version)
	if err != nil {
		return fmt.Errorf("unable to build runtime context: %w", err)
	}
	// TODO: Implement warnings hook, such as logging and HTTP sender
	for _, w := range d.context.Warnings {
		if w.Severity == "error" {
			log.Error().Any("ValidationWarning", w).Msg("")
		} else {
			log.Warn().Any("ValidationWarning", w).Msg("")
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

func (d *Dump) dataDump(ctx context.Context) error {
	// TODO: You should use pointer to dumpers.DumpTask instead
	tasks := make(chan dumpers.DumpTask, d.pgDumpOptions.Jobs)
	result := make(chan dump.Entry, d.pgDumpOptions.Jobs)
	defer close(result)

	log.Debug().Msgf("planned %d workers", d.pgDumpOptions.Jobs)
	eg, gtx := errgroup.WithContext(ctx)
	for j := 0; j < d.pgDumpOptions.Jobs; j++ {
		eg.Go(
			func(id int) func() error {
				return func() error {
					return d.dumpWorker(gtx, tasks, result, id+1)
				}
			}(j),
		)
	}

	// TODO: Implement LO dumping
	log.Warn().Msg("FIXME: implement Large Objects dumper")

	eg.Go(
		func() error {
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
		},
	)

	eg.Go(
		func() error {
			var tables, sequences, largeObjects []*toc.Entry
			for i := int32(0); !d.allTaskPushed.Load() || i < atomic.LoadInt32(&d.dumpTaskCount); i++ {
				select {
				case <-gtx.Done():
					return gtx.Err()
				case entry, ok := <-result:
					if entry == nil && ok {
						panic("unexpected entry nil pointer")
					}
					e, err := entry.Entry()
					if err != nil {
						return fmt.Errorf("error producing toc entry: %w", err)
					}
					switch v := entry.(type) {
					case *dump.Table:
						d.dumpedObjectSizes[e.DumpId] = storageDto.ObjectSizeStat{
							Original:   v.OriginalSize,
							Compressed: v.CompressedSize,
						}
						tables = append(tables, e)
					case *dump.Sequence:
						tables = append(tables, e)
					case *dump.LargeObject:
						tables = append(tables, e)
					default:
						return fmt.Errorf("unexpected toc entry type")
					}
				}
			}
			d.dataEntries = append(d.dataEntries, tables...)
			d.dataEntries = append(d.dataEntries, sequences...)
			d.dataEntries = append(d.dataEntries, largeObjects...)
			return nil
		},
	)

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}
	log.Debug().Msg("all the data have been dumped")
	return nil
}

func (d *Dump) mergeAndWriteToc(ctx context.Context, tx pgx.Tx) error {
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
	metadata, err := storageDto.NewMetadata(
		d.resultToc, d.tocFileSize, startedAt, completedAt, d.config.Dump.Transformation, d.dumpedObjectSizes,
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

func (d *Dump) BootstrapCustomTransformers(ctx context.Context) (err error) {
	for _, ctd := range d.config.CustomTransformers {
		var td *toolkit.Definition
		if ctd.Name == "" && !ctd.AutoDiscover {
			return fmt.Errorf("custom transformer without auto discovery must be defined staticly in the config")
		}
		if ctd.Executable == "" {
			return fmt.Errorf(`custom transformer "executable" parameter is required`)
		}

		if ctd.AutoDiscover {
			// Get custom transformer definition from stdout and override received data with config ctd
			args := make([]string, len(ctd.Args))
			copy(args, ctd.Args)
			args = append(args, custom.PrintConfigArgName)
			ctdd, err := custom.GetDynamicTransformerDefinition(ctd.Executable, args...)
			if err != nil {
				return fmt.Errorf("error getting dynamic transformer definition: %w", err)
			}
			ctd.Name = ctdd.Name
			ctd.Description = ctdd.Description
			ctd.Parameters = ctdd.Parameters
		}

		td = toolkit.NewDefinition(
			&toolkit.TransformerProperties{
				Name:        ctd.Name,
				Description: ctd.Description,
				IsCustom:    true,
			},
			custom.ProduceNewCmdTransformerFunction(ctd.Name, ctd.Executable, ctd.Args),
			ctd.Parameters...,
		)

		d.registry.MustRegister(td)
	}
	return nil
}

func (d *Dump) Run(ctx context.Context) (err error) {
	defer d.prune()
	startedAt := time.Now()

	if err := d.BootstrapCustomTransformers(ctx); err != nil {
		return fmt.Errorf("error bootstraping custom transformers: %w", err)
	}

	if err != nil {
		return fmt.Errorf("error building utils map: %w", err)
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

func (d *Dump) MergeTocEntries(schemaEntries []*toc.Entry, dataEntries []*toc.Entry) (
	[]*toc.Entry, error,
) {
	// TODO: Assign dependencies and sort entries in the same order
	res := make([]*toc.Entry, 0, len(schemaEntries)+len(dataEntries))

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
	res = append(res, dataEntries...)
	res = append(res, schemaEntries[postDataStart:]...)

	return res, nil
}

func (d *Dump) dumpWorker(
	ctx context.Context, tasks <-chan dumpers.DumpTask, result chan<- dump.Entry, id int,
) error {
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
