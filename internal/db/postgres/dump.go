package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/dumpers"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pg_catalog"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgrestore"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/transformers"
)

var defaultTypeMap = map[string]string{
	"aclitem":                     "",
	"any":                         "",
	"anyarray":                    "",
	"anycompatible":               "",
	"anycompatiblearray":          "",
	"anycompatiblemultirange":     "",
	"anycompatiblenonarray":       "",
	"anycompatiblerange":          "",
	"anyelement":                  "",
	"anyenum":                     "string",
	"anymultirange":               "",
	"anynonarray":                 "",
	"anyrange":                    "",
	"bigint":                      "int64",
	"bit":                         "bool",
	"bit varying":                 "",
	"boolean":                     "bool",
	"box":                         "",
	"bytea":                       "[]byte",
	"char":                        "",
	"character":                   "string",
	"character varying":           "string",
	"cid":                         "",
	"cidr":                        "",
	"circle":                      "",
	"cstring":                     "",
	"date":                        "string",
	"datemultirange":              "string",
	"daterange":                   "string",
	"double precision":            "float32",
	"event_trigger":               "",
	"fdw_handler":                 "",
	"gtsvector":                   "",
	"index_am_handler":            "",
	"inet":                        "string",
	"int2vector":                  "",
	"int4multirange":              "",
	"int4range":                   "",
	"int8multirange":              "",
	"int8range":                   "",
	"integer":                     "int32",
	"json":                        "",
	"jsonb":                       "",
	"jsonpath":                    "",
	"line":                        "",
	"lseg":                        "",
	"macaddr":                     "",
	"macaddr8":                    "",
	"money":                       "float64",
	"name":                        "string",
	"numeric":                     "", // Use variadic digit types
	"nummultirange":               "",
	"numrange":                    "",
	"path":                        "",
	"point":                       "",
	"polygon":                     "",
	"real":                        "float32",
	"smallint":                    "int16",
	"text":                        "string",
	"timestamp without time zone": "string", // TODO: Use date types or implement yourself
	"timestamp with time zone":    "string",
	"time without time zone":      "string",
	"time with time zone":         "string",
	"internal":                    "string",
	"interval":                    "string",
	"uuid":                        "uuid",
	"xml":                         "",
}

type Dump struct {
	dsn              string
	conn             *pgx.Conn
	pgDumpOptions    *pgdump.Options
	pgRestoreOptions *pgrestore.Options
	binPath          string
	curDumpId        int32
	st               storage.Storager
	dumpTaskCount    int32
	allTaskPushed    atomic.Bool
	typeMap          *pgtype.Map
}

func NewDump(binPath string, st storage.Storager) *Dump {
	return &Dump{
		binPath: binPath,
		st:      st,
	}
}

func (d *Dump) Connect(ctx context.Context, dsn string) error {

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	pgxdecimal.Register(conn.TypeMap())

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return err
	}

	d.typeMap = conn.TypeMap()
	d.conn = conn
	d.dsn = dsn
	return nil
}

func (d *Dump) setTableColumnsTransformers(ctx context.Context, tx pgx.Tx, table *domains.Table) error {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		    a.atttypid 	as typeoid,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod) as typename,
		  	a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	cfg := make(map[string]domains.Column, 0)
	for _, c := range table.Columns {
		cfg[c.Name] = c
	}

	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return fmt.Errorf("perform query: %w", err)
	}
	columns := make([]domains.Column, 0)
	for rows.Next() {
		column := domains.Column{}
		if err = rows.Scan(&column.Name, &column.TypeOid, &column.Type, &column.NotNull); err != nil {
			return fmt.Errorf("cannot scan column: %w", err)
		}

		if c, ok := cfg[column.Name]; ok {
			transformerConf := c.TransformConf
			makeTransformer, ok := transformers.TransformerMap[transformerConf.Name]
			if !ok {
				return fmt.Errorf("unnable to find transformer with name %s", transformerConf.Name)
			}
			column.TransformConf = transformerConf
			transformer, err := makeTransformer.NewTransformer(column.ColumnMeta, tx.Conn().TypeMap(), c.TransformConf.Params)
			if err != nil {
				return fmt.Errorf("unable to init transformer \"%s\": %w", transformerConf.Name, err)
			}
			column.Transformer = transformer
			table.HasTransformer = true
		}

		columns = append(columns, column)
	}

	table.Columns = columns
	return nil
}

func (d *Dump) objectList(ctx context.Context, tx pgx.Tx, confTables []domains.Table) ([]*domains.Table, []*domains.Sequence, error) {

	// making table map that will be used for merging pg catalog data and settings from config
	cfg := make(map[string]domains.Table, 0)
	for _, item := range confTables {
		cfg[fmt.Sprintf("%s.%s", item.Schema, item.Name)] = item
	}

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	query, err := pg_catalog.BuildTableSearchQuery(d.pgDumpOptions.Table, d.pgDumpOptions.ExcludeTable,
		d.pgDumpOptions.ExcludeTableData, d.pgDumpOptions.IncludeForeignData, d.pgDumpOptions.Schema,
		d.pgDumpOptions.ExcludeSchema)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	sequences := make([]*domains.Sequence, 0)
	tables := make([]*domains.Table, 0)
	defer rows.Close()
	for rows.Next() {
		var oid int
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool

		if err = rows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		); err != nil {
			return nil, nil, fmt.Errorf("unnable scan data: %w", err)
		}
		switch relKind {
		case 'S':
			sequences = append(sequences, &domains.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         oid,
				Owner:       owner,
				DumpId:      d.getDumpId(),
				LastValue:   lastVal,
				IsCalled:    isCalled,
				ExcludeData: excludeData,
			})
		case 'r':
			fallthrough
		case 'p':
			fallthrough
		case 'f':
			var columns []domains.Column
			t, ok := cfg[fmt.Sprintf("%s.%s", schemaName, name)]
			if ok {
				columns = t.Columns
			}
			table := &domains.Table{
				Oid:                  oid,
				Name:                 name,
				Schema:               schemaName,
				Columns:              columns,
				Query:                t.Query,
				Owner:                owner,
				DumpId:               d.getDumpId(),
				RelKind:              relKind,
				RootPtSchema:         rootPtSchema,
				RootPtName:           rootPtName,
				ExcludeData:          excludeData,
				LoadViaPartitionRoot: d.pgDumpOptions.LoadViaPartitionRoot,
			}

			tables = append(tables, table)
		default:
			return nil, nil, fmt.Errorf("unknown relkind \"%s\"", relKind)
		}
	}

	// Assign columns and transformers for table
	for _, table := range tables {
		if err := d.setTableColumnsTransformers(ctx, tx, table); err != nil {
			return nil, nil, err
		}
	}

	return tables, sequences, nil
}

func (d *Dump) startMainTx(ctx context.Context) (pgx.Tx, error) {
	var isolationLevel = "REPEATABLE READ"
	if d.pgDumpOptions.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}

	tx, err := d.conn.Begin(ctx)
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

func (d *Dump) getDumpId() int32 {
	atomic.AddInt32(&d.curDumpId, 1)
	return d.curDumpId
}

func (d *Dump) RunDump(ctx context.Context, opt *pgdump.Options, tableConfig []domains.Table) error {

	startedAt := time.Now()
	d.pgDumpOptions = opt
	pgDump := pgdump.NewPgDump(d.binPath)

	dsn, err := d.pgDumpOptions.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	if err := d.Connect(ctx, dsn); err != nil {
		return fmt.Errorf("cannot connect to db: %w", err)
	}
	defer d.conn.Close(ctx)

	tx, err := d.startMainTx(ctx)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	// Dump schema
	options := *d.pgDumpOptions
	options.Format = "d"
	options.SchemaOnly = true

	dumpDir := d.st.Getcwd()
	if err != nil {
		return fmt.Errorf("cannot get current working directory: %w", err)
	}
	options.FileName = dumpDir
	if err = pgDump.Run(ctx, &options); err != nil {
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
	d.curDumpId = schemaToc.MaxDumpId + 1

	tablesList, sequenceList, err := d.objectList(ctx, tx, tableConfig)
	if err != nil {
		return fmt.Errorf("cannot retreive sequence and table list: %w", err)
	}

	var largeObjects []*toc.Entry

	// TODO: You should use pointer to dumpers.DumpTask instead
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
	tocDataEntries := make([]*toc.Entry, 0, len(tablesList)+len(sequenceList)+len(largeObjects))
	eg.Go(func() error {
		tables := make([]*toc.Entry, 0, len(tablesList))
		sequences := make([]*toc.Entry, 0, len(sequenceList))
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
		tocDataEntries = append(tocDataEntries, tables...)
		tocDataEntries = append(tocDataEntries, sequences...)
		return nil
	})

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("at least one worker exited with error: %w", err)
	}

	log.Debug().Msg("merging toc entries")
	mergedTocs, err := d.MergeTocEntries(schemaToc.GetEntries(), tocDataEntries)
	if err != nil {
		return fmt.Errorf("unable to merge TOC files: %w", err)
	}

	log.Debug().Msg("writing built toc file")
	destTocFile, err := d.st.GetWriter(ctx, "toc.dat")
	if err != nil {
		return err
	}
	defer destTocFile.Close()
	schemaToc.SetEntries(mergedTocs)
	if err = toc.WriteFile(schemaToc, destTocFile); err != nil {
		return fmt.Errorf("error writing toc file: %w", err)
	}

	completedAt := time.Now()
	metadata, err := domains.NewMetadata(schemaToc.Header, schemaToc.GetEntries(),
		schemaToc.WrittenBytes, startedAt, completedAt,
		tableConfig,
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
