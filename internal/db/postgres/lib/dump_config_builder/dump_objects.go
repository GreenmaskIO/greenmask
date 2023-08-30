package dump_config_builder

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/domains/toclib"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/lib/pgdump"
)

func GetObjects(ctx context.Context, tx pgx.Tx, pgDumpOptions *pgdump.Options, tablesConfig map[toclib.Oid]*toclib.Table, dumpIdSeq *toclib.DumpId) ([]dump.TocDumper, error) {

	// Building relation search query using regexp adaptation rules and pre-defined query templates
	// TODO: Refactor it to gotemplate
	query, err := BuildTableSearchQuery(pgDumpOptions.Table, pgDumpOptions.ExcludeTable,
		pgDumpOptions.ExcludeTableData, pgDumpOptions.IncludeForeignData, pgDumpOptions.Schema,
		pgDumpOptions.ExcludeSchema)
	if err != nil {
		return nil, nil, err
	}

	rows, err := tx.Query(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("perform query: %w", err)
	}

	// Generate table objects
	sequences := make([]*toclib.Sequence, 0)
	tables := make([]*toclib.Table, 0)
	defer rows.Close()
	for rows.Next() {
		var oid toclib.Oid
		var lastVal int64
		var schemaName, name, owner, rootPtName, rootPtSchema string
		var relKind rune
		var excludeData, isCalled bool
		var ok bool

		if err = rows.Scan(&oid, &schemaName, &name, &owner, &relKind,
			&rootPtSchema, &rootPtName, &excludeData, &isCalled, &lastVal,
		); err != nil {
			return nil, nil, fmt.Errorf("unnable scan data: %w", err)
		}
		var table *toclib.Table

		switch relKind {
		case 'S':
			sequences = append(sequences, &toclib.Sequence{
				Name:        name,
				Schema:      schemaName,
				Oid:         toclib.Oid(oid),
				Owner:       owner,
				DumpId:      dumpIdSeq.GetDumpId(),
				LastValue:   lastVal,
				IsCalled:    isCalled,
				ExcludeData: excludeData,
			})
		case 'r':
			fallthrough
		case 'p':
			fallthrough
		case 'f':
			table, ok = tablesConfig[oid]
			if ok {
				// If table was discovered during Transformer validation - use that object instead of a new
				table.DumpId = dumpIdSeq.GetDumpId()
				table.ExcludeData = excludeData
				table.LoadViaPartitionRoot = pgDumpOptions.LoadViaPartitionRoot
			} else {
				// If not - create new table object
				table = &toclib.Table{
					Name:                 name,
					Schema:               schemaName,
					Oid:                  oid,
					Owner:                owner,
					DumpId:               dumpIdSeq.GetDumpId(),
					RelKind:              relKind,
					RootPtSchema:         rootPtSchema,
					RootPtName:           rootPtName,
					ExcludeData:          excludeData,
					LoadViaPartitionRoot: pgDumpOptions.LoadViaPartitionRoot,
				}
			}

			tables = append(tables, table)
		default:
			return nil, nil, fmt.Errorf("unknown relkind \"%s\"", relKind)
		}
	}

	// Assign columns and transformers for table
	for _, table := range tables {
		if err := setTableColumns(ctx, tx, table); err != nil {
			return nil, nil, err
		}
	}

	return tables, sequences, nil
}
