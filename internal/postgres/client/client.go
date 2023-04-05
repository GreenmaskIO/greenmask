package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/rs/zerolog/log"

	"github.com/wwoytenko/greenfuscator/internal/domains"
)

const backupDir = "/tmp/pg_dump_test"

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

type PostgresClient struct {
	typeMap         map[string]string
	dsn             string
	conn            *pgx.Conn
	binPath         string
	backupDirectory string
	snapshot        string
	backupId        int
	tocFile         io.Writer
}

func NewPostgresClient(binPath string) *PostgresClient {
	return &PostgresClient{
		typeMap: defaultTypeMap,
		binPath: binPath,
	}
}

func (pc *PostgresClient) Connect(ctx context.Context, dsn string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return err
	}

	pc.conn = conn
	pc.dsn = dsn
	return nil
}

func (pc *PostgresClient) startTx(ctx context.Context) (pgx.Tx, error) {
	tx, err := pc.conn.Begin(ctx)
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
	if err := row.Scan(&pc.snapshot); err != nil {
		tx.Rollback(ctx)
		return nil, fmt.Errorf("cannot export snapshot: %w", err)
	}

	return tx, nil
}

func (pc *PostgresClient) tablesList(ctx context.Context, tx pgx.Tx, filters map[string]string) ([]domains.Table, error) {
	tablesListQuery := `
		SELECT n.nspname                              as "Schema",
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

	tables := make([]domains.Table, 0)
	for rows.Next() {
		table := domains.Table{}

		if err := rows.Scan(&table.Schema, &table.Name, &table.Owner); err != nil {
			return nil, fmt.Errorf("unnable scan data: %w", err)
		}
		tables = append(tables, table)
	}

	return tables, nil
}

func (pc *PostgresClient) RunBackup(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context canceled: %w", ctx.Err())
	default:
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := pc.startTx(ctx)
	if err != nil {
		return fmt.Errorf("cannot prepare backup transaction: %w", err)
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Warn().Err(err)
		}
	}()

	tables, err := pc.tablesList(ctx, tx, map[string]string{})
	if err != nil {
		return fmt.Errorf("list table error: %w", err)
	}

	//  backupId int, table domains.Table
	for _, table := range tables {
		// TODO: Here must be goroutines and errgroup
		if err := pc.dumpTable(ctx, pc.backupId, &table); err != nil {
			return err
		}
	}

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
	//		* gzip data and store into BackupId.dat.gz
	// 9. Merge 3 TOC files into one
	// 10. Delete tmp data

	return nil
}

func (pc *PostgresClient) writeTocRecord(table *domains.Table) error {
	tocRecord, err := table.GetTocRecord()
	if err != nil {
		return fmt.Errorf("cannot get TOC record: %w", err)
	}
	if _, err := pc.tocFile.Write(tocRecord); err != nil {
		return fmt.Errorf("cannot write into TOC file: %w", err)
	}
	return nil
}

func (pc *PostgresClient) dumpTable(ctx context.Context, backupId int, table *domains.Table) error {
	var setIsolationLevelQuery = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
	var setSnapshotQuery = "SET TRANSACTION SNAPSHOT '$1'"
	var datFilePath = path.Join(backupDir, fmt.Sprintf("%d.dat.gz", backupId))

	datFile, err := os.Open(datFilePath)
	if err != nil {
		return fmt.Errorf("cannot open data file: %w", err)
	}
	defer datFile.Close()

	// Open file that wil contain table data

	// 1. Open a new connection
	// 2. Export snapshot
	conn, err := pgx.Connect(ctx, pc.dsn)
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

	if _, err := tx.Exec(ctx, setSnapshotQuery, pc.snapshot); err != nil {
		return fmt.Errorf("cannot import snapshot: %w", err)
	}

	if err := pc.writeTocRecord(table); err != nil {
		return fmt.Errorf("cannot write TOC record: %w", err)
	}

	frontend := conn.PgConn().Frontend()
	frontend.Send(&pgproto3.Query{
		String: fmt.Sprintf("COPY \"%s\".\"%s\" TO STDOUT", table.Schema, table.Name),
	})
	// TODO: Do we really need to flush?
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

			if _, err := datFile.Write(tupleData); err != nil {
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
