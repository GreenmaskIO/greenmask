package postgres

// TODO:
//		N. Dump data except some tables that cannot be
// 		N. Create DATA section with TOC records

import (
	"compress/gzip"
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
	"github.com/wwoytenko/greenfuscator/internal/storage"
	"golang.org/x/exp/slices"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/domains"
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
	snapshot  string
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

func (o *Obfuscator) startTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := o.conn.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to start transaction: %w", err)
	}

	rows, err := tx.Query(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
	if err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("cannot set transaction isolation level: %w", err)
	}
	rows.Close()

	row := tx.QueryRow(ctx, "SELECT pg_export_snapshot()")
	if err := row.Scan(&o.snapshot); err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("cannot export snapshot: %w", err)
	}

	return tx, nil
}

func (o *Obfuscator) getDumpId() int32 {
	o.curDumpId--
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
	// Algorithm:
	// N. Check directories exists
	// N. Create tmp dir for toc.dat - pre-data and post-data and data itself
	// 0. Create snapshot in REPEATABLE READ and use it during all the backup statements and calls
	// 1. Determine all tables to back up. Get their OID's, attributes and types
	// 2. If table has a rule we need to check:
	//		* Type violation - do we have this Masking function for this type
	//		* Does it have enough arguments, does arguments correct for this type
	// 3. Dump pg_dump -U postgres -d test -Fd --section=pre-data -f ./tmp/pre-data
	// 4. Dump pg_dump -U postgres -d test -Fd --section=pre-data -f ./tmp/post-data
	// 5. Upload pre-data and post-data
	//		* determine the all tables, keep their sequence
	//		* get min(pre-data.backupId) and min(post-data.backupId)
	// 7. Make the TOC file records in format for each table. Keep their order:
	//    11471; 1262 36497111 TABLE DATA - mydb postgres
	//	  3359; 0 16451 TABLE DATA public metrics test
	//
	//	  Where:
	//      * 3359 - internal sequence between pre-data and post-data
	//      * 0 - OID of pg_database catalog. Not required for section=data
	//      * 16451 - OID of TABLE/SEQUENCE/LARGE OBJECT
	//      * TABLE DATA - Object Type
	//		* public - Object Schema
	//      * metrics - Object Name (table name)
	//      * test - Object Owner
	//
	// For details, please refer https://www.postgresql.org/message-id/20160126173717.GA565213%40alvherre.pgsql
	// 8. Run COPY command
	//      * apply the transformers for each required attribute
	//		* gzip data and store into DumpId.dat.gz
	// 9. Merge 3 TOC files into one
	// 10. Delete tmp data
	o.curDumpId = int32(maxInt)

	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled: %w", ctx.Err())
	default:
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := o.Connect(ctx); err != nil {
		return fmt.Errorf("cannot connect to db: %w", err)
	}

	tx, err := o.startTx(ctx)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	sequenceList, err := o.sequenceList(ctx, tx)
	if err != nil {
		return fmt.Errorf("cannot retreive sequence list: %w", err)
	}

	tablesList, err := o.tablesList(ctx, tx, tableConfig)
	if err != nil {
		return fmt.Errorf("cannot retreive table list: %w", err)
	}

	// N. Make --schemaonly dump in original dir
	// N. Read Toc data and calculate  MinBackupId and MaxBackupId
	// N. Start Backup Tables into dir using backup order
	// N. Generate sequences setting up by
	// N. Backing up blobs, change the Backup ID if is not suitable for free backupId rage

	// Dump schema
	options := *o.options
	options.Format = "d"
	options.Verbose = true
	options.SchemaOnly = true
	options.Snapshot = o.snapshot

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
	defer srcTocFile.Close()
	schemaToc, err := toc.ReadFile(srcTocFile)
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}

	for idx := range tablesList {
		log.Debug().Msgf("performing table dump for table %s.%s with dumpId %d", tablesList[idx].Name,
			tablesList[idx].Schema, tablesList[idx].DumpId)
		if err = o.dumpTable(ctx, &tablesList[idx]); err != nil {
			return fmt.Errorf("unnable to perform dump of table %s.%s: %w", tablesList[idx].Schema, tablesList[idx].Name, err)
		}
	}

	log.Debug().Msg("build toc data section")
	dataEntries, err := o.buildDataSection(schemaToc.GetEntries(), tablesList, sequenceList, nil)
	if err != nil {
		return fmt.Errorf("cannot build data section: %w", err)
	}

	log.Debug().Msg("merge toc entries")
	mergedTocs, err := o.MergeTocEntries(schemaToc.GetEntries(), dataEntries)
	if err != nil {
		return fmt.Errorf("unable to merge TOC files: %w", err)
	}

	log.Debug().Msg("write built toc file")
	destTocFile, err := o.st.GetWriter(ctx, "toc.dat")
	if err != nil {
		return err
	}
	defer destTocFile.Close()
	schemaToc.SetEntries(mergedTocs)
	if err = toc.WriteFile(schemaToc, destTocFile); err != nil {
		return fmt.Errorf("error writing toc file: %w", err)
	}

	return nil
}

func (o *Obfuscator) buildDataSection(preData []toc.Entry, tables []domains.Table,
	sequences []domains.Sequence, largeObjects *domains.LargeObjects) ([]toc.Entry, error) {

	log.Warn().Msgf("FIXME: implement Large Objects dumping")

	res := make([]toc.Entry, 0, len(tables)+len(sequences)+1)

	sequenceOrder := o.GetSequenceOrder(preData)
	tableOrder := o.GetTableOrder(preData)

	for _, tableDef := range tableOrder {
		for _, tableData := range tables {
			if *tableDef.Namespace == tableData.Schema && *tableDef.Tag == tableData.Name {
				tableData.Dependencies = []int32{tableDef.DumpId}
				entry, err := tableData.GetTocEntry()
				if err != nil {
					return nil, fmt.Errorf("cannot get table TOC entry: %w", err)
				}
				res = append(res, *entry)
				break
			}
		}
	}

	for _, sequenceDef := range sequenceOrder {
		for _, sequenceData := range sequences {
			if *sequenceDef.Namespace == sequenceData.Schema && *sequenceDef.Tag == sequenceData.Name {
				sequenceData.Dependencies = []int32{sequenceDef.DumpId}
				entry, err := sequenceData.GetTocEntry()
				if err != nil {
					return nil, fmt.Errorf("cannot get sequence TOC entry: %w", err)
				}
				res = append(res, *entry)
				break
			}
		}
	}

	return res, nil
}

func (o *Obfuscator) dumpTable(ctx context.Context, table *domains.Table) error {
	var setIsolationLevelQuery = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", o.snapshot)
	//var datFilePath = path.Join(datDir, )

	datFile, err := o.st.GetWriter(ctx, fmt.Sprintf("%d.dat.gz", table.DumpId))
	if err != nil {
		return fmt.Errorf("cannot open data file: %w", err)
	}
	defer datFile.Close()
	writer := gzip.NewWriter(datFile)
	defer writer.Close()

	// Open file that wil contain table data

	// 1. Open a new connection
	// 2. Export snapshot
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

	if _, err := tx.Exec(ctx, setIsolationLevelQuery); err != nil {
		return fmt.Errorf("unable to set transaction isolation level: %w", err)
	}

	if _, err := tx.Exec(ctx, setSnapshotQuery); err != nil {
		return fmt.Errorf("cannot import snapshot: %w", err)
	}

	frontend := conn.PgConn().Frontend()
	frontend.Send(&pgproto3.Query{
		String: fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", table.Schema, table.Name),
	})

	if err := frontend.Flush(); err != nil {
		return err
	}

	for {
		msg, err := frontend.Receive()
		if err != nil {
			// TODO: You must send asynchronous message that you have stopped in error
			return fmt.Errorf("unable to perform copy query: %w", err)
		}
		switch v := msg.(type) {
		case *pgproto3.CopyOutResponse:
			// CopyOutResponse does not matter for us in TEXTUAL MODES
			// https://www.postgresql.org/docs/current/sql-copy.html
		case *pgproto3.CopyData:
			tupleData := v.Data
			if table.HasMasker {
				tupleData, err = table.TransformTuple(tupleData)
				if err != nil {
					return fmt.Errorf("cannot convert plain data to tuple: %w", err)
				}
			}

			// TODO: Maybe you should check the count of written bytes
			if _, err := writer.Write(tupleData); err != nil {
				return fmt.Errorf("cannot store data into dat file: %w", err)
			}

		case *pgproto3.CopyDone:
		case *pgproto3.CommandComplete:
		case *pgproto3.ReadyForQuery:
			return nil
		default:
			return fmt.Errorf("unknown backup message %+v", v)
		}
	}
}

func (o *Obfuscator) GetTableOrder(entries []toc.Entry) []toc.Entry {
	tableOrder := make([]toc.Entry, 0)
	for _, item := range entries {
		if item.Section == toc.SectionPreData && *(item.Desc) == "TABLE" {
			tableOrder = append(tableOrder, item)
		}

	}
	return tableOrder
}

func (o *Obfuscator) GetSequenceOrder(entries []toc.Entry) []toc.Entry {
	tableOrder := make([]toc.Entry, 0)
	for _, item := range entries {
		if item.Section == toc.SectionPreData && *(item.Desc) == "SEQUENCE" {
			tableOrder = append(tableOrder, item)
		}
	}
	return tableOrder
}

func (o *Obfuscator) MergeTocEntries(schema, data []toc.Entry) ([]toc.Entry, error) {
	res := make([]toc.Entry, 0, len(schema)+len(data))

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
	// Push data between post and pred
	res = append(res, data...)
	res = append(res, schema[postDataStart:]...)

	return res, nil
}
