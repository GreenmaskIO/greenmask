package postgres

// TODO:
//		N. Dump data except some tables that cannot be
// 		N. Create DATA section with TOC records

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/dumpers"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"github.com/wwoytenko/greenfuscator/internal/transformers"
)

const (
	maxInt = 2147483647
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

type TableDataRange struct {
	TablesOrder []string
	MaxBackupId int
	MinBackupId int
}

type Obfuscator struct {
	typeMap   map[string]string
	dsn       string
	conn      *pgx.Conn
	backupId  int
	options   *pgdump.Options
	pgDump    *pgdump.PgDump
	curDumpId int32
	st        storage.Storager
}

func NewObfuscator(binPath string, options *pgdump.Options, st storage.Storager) *Obfuscator {
	return &Obfuscator{
		typeMap: defaultTypeMap,
		options: options,
		pgDump:  pgdump.NewPgDump(binPath),
		st:      st,
	}
}

func (o *Obfuscator) Connect(ctx context.Context) error {
	dsn, err := o.options.GetPgDSN()
	if err != nil {
		return fmt.Errorf("cannot build connection string: %w", err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return err
	}

	o.conn = conn
	o.dsn = dsn
	return nil
}

func (o *Obfuscator) sequenceList(ctx context.Context, tx pgx.Tx) ([]domains.Sequence, error) {
	// TODO: Provide filter rules - exclude seq or schema, etc.
	tablesListQuery := `
		SELECT n.nspname                              as "Schema",
			   c.relname                              as "Name",
			   pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
			   CASE
				   WHEN pg_sequence_last_value(c.oid::regclass) ISNULL THEN
					   FALSE
				   ELSE
					   TRUE
				   END                                AS "IsCalled",
			coalesce(pg_sequence_last_value(c.oid::regclass), s.seqstart) AS "LastVal"
		FROM pg_catalog.pg_class c
				 JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				 JOIN pg_catalog.pg_sequence s ON c.oid = s.seqrelid
		WHERE c.relkind IN ('S', '')
		  AND n.nspname <> 'pg_catalog'
		  AND n.nspname !~ '^pg_toast'
		  AND n.nspname <> 'information_schema'
		ORDER BY 1, 2;
	`

	rows, err := tx.Query(ctx, tablesListQuery)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	sequences := make([]domains.Sequence, 0)
	defer rows.Close()
	for rows.Next() {
		sequence := domains.Sequence{}
		sequence.DumpId = o.getDumpId()
		if err := rows.Scan(&sequence.Schema, &sequence.Name,
			&sequence.Owner, &sequence.IsCalled, &sequence.LastValue); err != nil {
			return nil, fmt.Errorf("unnable scan data: %w", err)
		}
		sequences = append(sequences, sequence)
	}

	return sequences, nil
}

func (o *Obfuscator) startMainTx(ctx context.Context) (pgx.Tx, error) {
	var isolationLevel = "REPEATABLE READ"
	if o.options.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}

	tx, err := o.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}

	rows, err := tx.Query(ctx, fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel))
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("cannot set transaction isolation level: %w", err)
	}
	rows.Close()

	if o.options.Snapshot == "" {
		log.Debug().Msg("performing snapshot export")
		row := tx.QueryRow(ctx, "SELECT pg_export_snapshot()")
		if err := row.Scan(&o.options.Snapshot); err != nil {
			tx.Rollback(ctx)
			return nil, fmt.Errorf("cannot export snapshot: %w", err)
		}
	} else {
		var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", o.options.Snapshot)
		log.Debug().Msgf("performing %s snapshot import", o.options.Snapshot)
		if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
			return nil, fmt.Errorf("cannot import snapshot: %w", err)
			tx.Rollback(ctx)
		}
	}

	return tx, nil
}

func (o *Obfuscator) getDumpId() int32 {
	o.curDumpId++
	return o.curDumpId
}

func (o *Obfuscator) tablesList(ctx context.Context, tx pgx.Tx, confTables []domains.Table) ([]domains.Table, error) {
	tablesListQuery := `
		SELECT c.oid::TEXT::INT, 
		       n.nspname                              as "Schema",
			   c.relname                              as "Name",
			   pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
		FROM pg_catalog.pg_class c
				 LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
				 LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
		WHERE c.relkind IN ('r', 'p', '')
		  AND n.nspname <> 'pg_catalog'
		  AND n.nspname !~ '^pg_toast'
		  AND n.nspname <> 'information_schema'
		ORDER BY 1, 2;
	`

	rows, err := tx.Query(ctx, tablesListQuery)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	tables := make([]domains.Table, 0)
	defer rows.Close()
	for rows.Next() {
		table := domains.Table{}
		table.DumpId = o.getDumpId()
		if err := rows.Scan(&table.Oid, &table.Schema, &table.Name, &table.Owner); err != nil {
			return nil, fmt.Errorf("unnable scan data: %w", err)
		}
		tables = append(tables, table)
	}

	// TODO:
	// 	1. Find table in the list if it is exists - return it and get transformers

	// Assign columns to each table
	for idx, _ := range tables {
		var tableConf *domains.Table
		confIdx := slices.IndexFunc[domains.Table](confTables, func(v domains.Table) bool {
			if tables[idx].Schema == v.Schema && tables[idx].Name == v.Name {
				return true
			}
			return false
		})

		if confIdx != -1 {
			tables[idx].HasMasker = true
			tableConf = &confTables[confIdx]
		}

		columns, err := o.getTableColumns(ctx, tx, &tables[idx], tableConf)
		if err != nil {
			return nil, fmt.Errorf("unnable to fill table colimns: %w", err)
		}
		tables[idx].Columns = columns
	}

	return tables, nil
}

func (o *Obfuscator) getTableColumns(ctx context.Context, tx pgx.Tx, table *domains.Table, tableConf *domains.Table) ([]domains.Column, error) {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod),
		  	a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	rows, err := tx.Query(ctx, tableColumnsQuery, table.Oid)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}
	columns := make([]domains.Column, 0)
	for rows.Next() {
		column := domains.Column{}
		if err = rows.Scan(&column.Name, &column.Type, &column.NotNull); err != nil {
			return nil, fmt.Errorf("cannot scan column: %w", err)
		}

		if tableConf != nil {
			confIdx := slices.IndexFunc[domains.Column](tableConf.Columns, func(v domains.Column) bool {
				if column.Name == v.Name {
					return true
				}
				return false
			})
			if confIdx != -1 {
				transformerConf := tableConf.Columns[confIdx].TransformConf
				makeTransformer, ok := transformers.TransformerMap[transformerConf.Name]
				if !ok {
					return nil, fmt.Errorf("unnable to find transformer with name %s", transformerConf.Name)
				}
				column.TransformConf = transformerConf
				transformer, err := makeTransformer.NewTransformer(column.ColumnMeta, column.TransformConf.Params)
				if err != nil {
					return nil, fmt.Errorf("unable to init transformer \"%s\": %w", transformerConf.Name, err)
				}
				column.Transformer = transformer
			}
		}

		columns = append(columns, column)
	}

	return columns, nil
}

func (o *Obfuscator) RunBackup(ctx context.Context, tableConfig []domains.Table) error {

	startedAt := time.Now()

	if err := o.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to db: %w", err)
	}

	tx, err := o.startMainTx(ctx)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	// Dump schema
	options := *o.options
	options.Format = "d"
	options.Verbose = true
	options.SchemaOnly = true

	dumpDir, err := o.st.Getcwd(ctx)
	if err != nil {
		return fmt.Errorf("cannot get current working directory: %w", err)
	}
	options.FileName = dumpDir
	if err = o.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	log.Debug().Msg("renaming toc file")
	if err := o.st.Rename(ctx, "toc.dat", "~toc.dat"); err != nil {
		return fmt.Errorf("cannot rename toc.dat file: %w", err)
	}

	log.Debug().Msg("reading schema section")
	srcTocFile, err := o.st.GetReader(ctx, "~toc.dat")
	if err != nil {
		return err
	}
	defer func() {
		srcTocFile.Close()
		if err := o.st.Delete(ctx, "~toc.dat", false); err != nil {
			log.Warn().Err(err).Msgf("unable to delete temp file")
		}
	}()
	schemaToc, err := toc.ReadFile(srcTocFile)
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}
	o.curDumpId = schemaToc.MaxDumpId + 1

	sequenceList, err := o.sequenceList(ctx, tx)
	if err != nil {
		return fmt.Errorf("cannot retreive sequence list: %w", err)
	}

	tablesList, err := o.tablesList(ctx, tx, tableConfig)
	if err != nil {
		return fmt.Errorf("cannot retreive table list: %w", err)
	}

	var largeObjects []*toc.Entry

	tasks := make(chan dumpers.DumpTask, o.options.Jobs)
	result := make(chan *toc.Entry, o.options.Jobs)
	defer close(result)

	log.Debug().Msgf("planned %d workers", o.options.Jobs)
	eg, gtx := errgroup.WithContext(ctx)
	for j := 0; j < o.options.Jobs; j++ {
		eg.Go(func(id int) func() error {
			return func() error {
				return o.dumpWorker(gtx, tasks, result, id+1)
			}
		}(j))
	}

	// TODO: Implement LO dumping
	log.Debug().Msg("FIXME: implement Large Objects dumper")

	eg.Go(func() error {
		defer close(tasks)
		for _, table := range tablesList {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}
			tasks <- dumpers.NewTableDumper(table)
		}
		for _, sequence := range sequenceList {
			select {
			case <-gtx.Done():
				return gtx.Err()
			default:
			}
			tasks <- dumpers.NewSequenceDumper(sequence)
		}
		return nil
	})

	var originalSize, compressedSize int64
	tocDataEntries := make([]*toc.Entry, 0, len(tablesList)+len(sequenceList)+len(largeObjects))
	eg.Go(func() error {
		tables := make([]*toc.Entry, 0, len(tablesList))
		sequences := make([]*toc.Entry, 0, len(sequenceList))
		for i := 0; i < len(tablesList)+len(sequenceList)+len(largeObjects); i++ {
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
	mergedTocs, err := o.MergeTocEntries(schemaToc.GetEntries(), tocDataEntries)
	if err != nil {
		return fmt.Errorf("unable to merge TOC files: %w", err)
	}

	log.Debug().Msg("writing built toc file")
	destTocFile, err := o.st.GetWriter(ctx, "toc.dat")
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
	)
	if err != nil {
		return fmt.Errorf("unable build metadata: %w", err)
	}
	meta, err := o.st.GetWriter(ctx, "metadata.json")
	if err != nil {
		return fmt.Errorf("unable to open metadata file: %w", err)
	}
	defer meta.Close()
	if err := json.NewEncoder(meta).Encode(metadata); err != nil {
		return fmt.Errorf("unable to write metadata: %w", err)
	}

	return nil
}

func (o *Obfuscator) MergeTocEntries(schema, data []*toc.Entry) ([]*toc.Entry, error) {
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

func (o *Obfuscator) dumpWorker(ctx context.Context, tasks <-chan dumpers.DumpTask, result chan<- *toc.Entry, id int) error {
	var isolationLevel = "REPEATABLE READ"
	if o.options.SerializableDeferrable {
		isolationLevel = "SERIALIZABLE DEFERRABLE"
	}
	var setIsolationLevelQuery = fmt.Sprintf("SET TRANSACTION ISOLATION LEVEL %s", isolationLevel)
	var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", o.options.Snapshot)

	for task := range tasks {
		log.Debug().Msgf("worker %d: dumping %s", id, task.DebugInfo())
		select {
		case <-ctx.Done():
			log.Debug().Msgf("worker %d: dumping %s: existed due to cancelled context: %w", id, task.DebugInfo(), ctx.Err())
			return ctx.Err()
		default:
		}
		conn, err := pgx.Connect(ctx, o.dsn)
		if err != nil {
			return fmt.Errorf("cannot connecti to server: %w", err)
		}
		defer conn.Close(ctx)

		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("cannot start transaction: %w", err)
		}
		defer tx.Rollback(ctx)

		if !o.options.NoSynchronizedSnapshots {
			if _, err := tx.Exec(ctx, setIsolationLevelQuery); err != nil {
				return fmt.Errorf("unable to set transaction isolation level: %w", err)
			}

			if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
				return fmt.Errorf("cannot import snapshot: %w", err)
			}
		}

		entry, err := task.Execute(ctx, tx, o.st)
		if err != nil {
			return err
		}
		result <- entry
		log.Debug().Msgf("worker %d: %s: dumping is done", id, task.DebugInfo())
	}

	return nil
}
