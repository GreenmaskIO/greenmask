package postgres

import (
	"compress/gzip"
	"context"
	//"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/slices"
	"os"
	"path"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/toc"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const (
	pgDumpDir                     = "pg_dump"
	pgDumpPreDataDir              = "pg_dump/predata"
	pgDumpDataDir                 = "pg_dump/data"
	pgDumpPostDataDir             = "pg_dump/postdata"
	dirPermissions    os.FileMode = 0750
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
}

func NewObfuscator(binPath string, options *pgdump.Options) *Obfuscator {
	return &Obfuscator{
		typeMap: defaultTypeMap,
		options: options,
		pgDump:  pgdump.NewPgDump(binPath),
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

func (o *Obfuscator) tablesList(ctx context.Context, tx pgx.Tx, filters map[string]string) ([]domains.Table, error) {
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

	// Assign columns to each table
	for idx, _ := range tables {
		columns, err := o.getTableColumns(ctx, tx, &tables[idx])
		if err != nil {
			return nil, fmt.Errorf("unnable to fill table colimns: %w", err)
		}
		tables[idx].Columns = columns
	}

	return tables, nil
}

func (o *Obfuscator) getTableColumns(ctx context.Context, tx pgx.Tx, t *domains.Table) ([]domains.Column, error) {
	tableColumnsQuery := `
		SELECT 
		    a.attname,
		  	pg_catalog.format_type(a.atttypid, a.atttypmod),
		  	a.attnotnull
		FROM pg_catalog.pg_attribute a
		WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY a.attnum
	`

	rows, err := tx.Query(ctx, tableColumnsQuery, t.Oid)
	if err != nil {
		return nil, fmt.Errorf("perform query: %w", err)
	}
	columns := make([]domains.Column, 0)
	for rows.Next() {
		column := domains.Column{}
		if err = rows.Scan(&column.Name, &column.Type, &column.NotNull); err != nil {
			return nil, fmt.Errorf("cannot scan column: %w", err)
		}
		columns = append(columns, column)
	}

	return columns, nil
}

func (o *Obfuscator) RunBackup(ctx context.Context) error {
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
	//      * apply the masker for each required attribute
	//		* gzip data and store into DumpId.dat.gz
	// 9. Merge 3 TOC files into one
	// 10. Delete tmp data
	maxInt := 2147483647
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

	tablesList, err := o.tablesList(ctx, tx, map[string]string{})
	if err != nil {
		return err
	}
	log.Debug().Msgf("tablesList = %+v\n", tablesList)

	// N. Make --schemaonly dump in original dir
	// N. Read Toc data and calculate  MinBackupId and MaxBackupId
	// N. Start Backup Tables into dir using backup order
	// N. Generate sequences setting up by
	// N. Backing up blobs, change the Backup ID if is not suitable for free backupId rage

	// N. Create subdirectory - for original, masking
	if err = o.createDirectories(); err != nil {
		return err
	}

	// Dump pre data
	options := *o.options
	options.Format = "d"
	options.Verbose = true
	options.Section = "pre-data"
	options.FileName = path.Join(o.options.FileName, pgDumpPreDataDir)
	if err = o.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	// Dump pre data
	options.Section = "data"
	options.FileName = path.Join(o.options.FileName, pgDumpDataDir)
	if err = o.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	// Dump data data
	options.Section = "post-data"
	options.FileName = path.Join(o.options.FileName, pgDumpPostDataDir)
	if err = o.pgDump.Run(ctx, &options); err != nil {
		return err
	}

	// Read predata TOC
	log.Debug().Msg("Backing up data section")
	preDataToc, err := toc.ReadFile(path.Join(o.options.FileName, pgDumpPreDataDir, "toc.dat"))
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}

	// Read data TOC
	log.Debug().Msg("Backing up data section")
	dataToc, err := toc.ReadFile(path.Join(o.options.FileName, pgDumpDataDir, "toc.dat"))
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}

	log.Debug().Msg("Backing up post-data section")
	postDataToc, err := toc.ReadFile(path.Join(o.options.FileName, pgDumpPostDataDir, "toc.dat"))
	if err != nil {
		return fmt.Errorf("error reading toc file: %w", err)
	}

	tableOrder := o.GetTableOrder(preDataToc.GetEntries())
	log.Printf("%+v\n", tableOrder)

	// Performing TSV dump
	for idx := range tablesList {
		if err = o.dumpTable(ctx, o.options.FileName, &tablesList[idx]); err != nil {
			return fmt.Errorf("unnable to perform dump of table %s.%s: %w", tablesList[idx].Schema, tablesList[idx].Name, err)
		}
	}

	//o.setMaxDumpId(preDataToc.GetEntries(), postDataToc.GetEntries(), tablesList)

	// Replacing dat
	datEntries := dataToc.GetEntries()
	for eIdx := range datEntries {
		if datEntries[eIdx].Section == 3 {
			for tIdx := range tablesList {
				if int(datEntries[eIdx].CatalogId.Oid) == tablesList[tIdx].Oid {
					datEntries[eIdx].DumpId = tablesList[tIdx].DumpId
					fileName := fmt.Sprintf("%d.dat.gz", tablesList[tIdx].DumpId)
					datEntries[eIdx].FileName = &fileName
					break
				}
			}
		}

	}

	mergedTocs, err := o.MergeTocEntries(preDataToc.GetEntries(), dataToc.GetEntries(), postDataToc.GetEntries())
	if err != nil {
		return fmt.Errorf("unable to merge TOC files: %w", err)
	}
	log.Debug().Msgf("mergedToc = %+v\n", mergedTocs)

	// Read post data TOC
	targetTocFilePath := path.Join(o.options.FileName, "toc.dat")
	preDataToc.SetEntries(mergedTocs)
	if err = toc.WriteFile(preDataToc, targetTocFilePath); err != nil {
		return fmt.Errorf("error writing toc file: %w", err)
	}

	return nil
}

func (o *Obfuscator) createDirectories() error {
	// TODO: Don't forget to check is it directory
	dir := o.options.FileName
	log.Debug().Msg("create subdirectories")

	for _, dirName := range []string{pgDumpDir, pgDumpPreDataDir, pgDumpDataDir, pgDumpPostDataDir} {
		if err := os.Mkdir(path.Join(dir, dirName), dirPermissions); err != nil {
			return fmt.Errorf("cannot create pg_dump directories: %w", err)
		}
	}

	return nil
}

func (o *Obfuscator) setMaxDumpId(preData, postData []toc.Entry, tables []domains.Table) {
	var maxDumpId int32
	for _, item := range preData {
		if item.DumpId > maxDumpId {
			maxDumpId = item.DumpId
		}
	}

	for _, item := range postData {
		if item.DumpId > maxDumpId {
			maxDumpId = item.DumpId
		}
	}

	// TODO: It is just for testing purposes. Determine the algorithm first
	maxDumpId += int32(len(tables)) * 4
	o.curDumpId = maxDumpId

	for idx := range tables {
		o.curDumpId++
		tables[idx].DumpId = o.curDumpId
	}

}

// TODO: You need to review this code. It stuck on receiving from the Frontend
func (o *Obfuscator) dumpTable(ctx context.Context, datDir string, table *domains.Table) error {
	var setIsolationLevelQuery = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	var setSnapshotQuery = fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", o.snapshot)
	var datFilePath = path.Join(datDir, fmt.Sprintf("%d.dat.gz", table.DumpId))

	datFile, err := os.Create(datFilePath)
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
			// TODO: Consider how CopyOutResponse would be helpful
			log.Debug().Msgf("received CopyOutResponse: %+v", v)
		case *pgproto3.CopyData:
			tupleData := v.Data
			if table.HasMasker {
				tuple, err := table.MakeTuple(tupleData)
				if err != nil {
					return fmt.Errorf("cannot convert plain data to tuple: %w", err)
				}

				if err := tuple.MaskTuple(); err != nil {
					return fmt.Errorf("cannot mask tuple: %w", err)
				}
				tupleData = tuple.GetMaskedTuple()
			}

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

func (o *Obfuscator) GetTableOrder(entries []toc.Entry) []string {
	tableOrder := make([]string, 0)
	for _, item := range entries {
		if *(item.Desc) == "TABLE" {
			tableOrder = append(tableOrder, fmt.Sprintf("%s.%s", *item.Namespace, *item.Tag))
		}

	}
	return tableOrder
}

func (o *Obfuscator) MergeTocEntries(preData, data, postData []toc.Entry) ([]toc.Entry, error) {
	allEntries := make([]toc.Entry, 0)
	allEntries = append(allEntries, preData...)
	allEntries = append(allEntries, data...)
	allEntries = append(allEntries, postData...)

	res := make([]toc.Entry, 0, len(allEntries))

	for _, item := range allEntries {
		tocExists := slices.ContainsFunc[toc.Entry](res, func(entry toc.Entry) bool {
			if entry.DumpId == item.DumpId {
				return true
			}
			return false
		})
		if !tocExists {
			res = append(res, item)
		}
	}
	return res, nil
}
