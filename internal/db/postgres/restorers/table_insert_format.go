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

package restorers

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TableRestorerInsertFormat struct {
	*restoreBase
	Table            *toolkit.Table
	query            string
	globalExclusions *domains.GlobalDataRestorationErrorExclusions
	tableExclusion   *domains.TablesDataRestorationErrorExclusions
}

func NewTableRestorerInsertFormat(
	entry *toc.Entry, t *toolkit.Table, st storages.Storager, opt *pgrestore.DataSectionSettings,
	exclusions *domains.DataRestorationErrorExclusions,
) *TableRestorerInsertFormat {

	var (
		tableExclusion  *domains.TablesDataRestorationErrorExclusions
		globalExclusion *domains.GlobalDataRestorationErrorExclusions
	)

	if exclusions != nil {
		globalExclusion = exclusions.Global
		idx := slices.IndexFunc(exclusions.Tables, func(t *domains.TablesDataRestorationErrorExclusions) bool {
			schema := fmt.Sprintf(`"%s"`, t.Schema)
			if len(t.Schema) > 0 && t.Schema[0] != '"' {
				schema = fmt.Sprintf(`"%s"`, t.Schema)
			}
			table := fmt.Sprintf(`"%s"`, t.Name)
			if len(t.Name) > 0 && t.Name[0] != '"' {
				table = fmt.Sprintf(`"%s"`, t.Name)
			}
			return (schema == *entry.Namespace) && table == *entry.Tag
		})
		if idx != -1 {
			tableExclusion = exclusions.Tables[idx]
		}
	}

	return &TableRestorerInsertFormat{
		restoreBase:      newRestoreBase(entry, st, opt),
		Table:            t,
		globalExclusions: globalExclusion,
		tableExclusion:   tableExclusion,
	}
}

func (td *TableRestorerInsertFormat) GetEntry() *toc.Entry {
	return td.entry
}

func (td *TableRestorerInsertFormat) Execute(ctx context.Context, conn *pgx.Conn) error {

	r, complete, err := td.getObject(ctx)
	if err != nil {
		return fmt.Errorf("cannot get storage object: %w", err)
	}
	defer complete()

	if err = td.streamInsertData(ctx, conn, r); err != nil {
		if td.opt.ExitOnError {
			return fmt.Errorf("error streaming pgcopy data: %w", err)
		}
		log.Warn().Err(err).Msg("error streaming pgcopy data")
		return nil
	}
	return nil
}

func (td *TableRestorerInsertFormat) streamInsertData(ctx context.Context, conn *pgx.Conn, r io.Reader) error {
	// Streaming pgcopy data from table dump
	buf := bufio.NewReader(r)

	row := pgcopy.NewRow(pgcopy.UseDynamicSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := reader.ReadLine(buf, nil)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error readimg from table dump: %w", err)
		}
		if isTerminationSeq(line) {
			break
		}
		if err = row.Decode(line); err != nil {
			return fmt.Errorf("error decoding line: %w", err)
		}

		if err = td.insertDataOnConflictDoNothing(ctx, conn, row); err != nil {
			if !td.isErrorAllowed(err) {
				return fmt.Errorf("error inserting data: %w", err)
			} else {
				log.Debug().Err(err).Msgf("skipping error because in insert_error_exclusions: error inserting data: %s", td.DebugInfo())
			}
		}

	}
	return nil
}

func (td *TableRestorerInsertFormat) generateInsertStmt(onConflictDoNothing bool) string {
	var placeholders []string
	var columnNames []string
	columns := getRealColumns(td.Table.Columns)
	for i := 0; i < len(columns); i++ {
		column := fmt.Sprintf(`"%s"`, columns[i].Name)
		columnNames = append(columnNames, column)
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	var onConflict string
	if onConflictDoNothing {
		onConflict = " ON CONFLICT DO NOTHING"
	}

	overridingSystemValue := ""
	if td.opt.OverridingSystemValue {
		overridingSystemValue = "OVERRIDING SYSTEM VALUE "
	}

	tableName := *td.entry.Tag
	tableSchema := *td.entry.Namespace

	if td.Table.RootPtOid != 0 {
		tableName = td.Table.RootPtName
		tableSchema = td.Table.RootPtSchema
	}

	res := fmt.Sprintf(
		`INSERT INTO %s.%s (%s) %sVALUES(%s)%s`,
		tableSchema,
		tableName,
		strings.Join(columnNames, ", "),
		overridingSystemValue,
		strings.Join(placeholders, ", "),
		onConflict,
	)
	return res
}

func (td *TableRestorerInsertFormat) insertDataOnConflictDoNothing(
	ctx context.Context, conn *pgx.Conn, row *pgcopy.Row,
) error {
	if td.query == "" {
		td.query = td.generateInsertStmt(td.opt.OnConflictDoNothing)
	}

	// TODO: The implementation based on Exec is not efficient for bulk inserts.
	// 	 Consider rewrite to string literal that contains generated statement instead of using prepared statement
	//	 in driver
	_, err := conn.Exec(ctx, td.query, getAllArguments(row)...)
	if err != nil {
		return err
	}
	return nil
}

func (td *TableRestorerInsertFormat) isErrorAllowed(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	if td.tableExclusion != nil {
		if slices.Contains(td.tableExclusion.ErrorCodes, pgErr.Code) {
			return true
		}
		if slices.Contains(td.tableExclusion.Constraints, pgErr.ConstraintName) {
			return true
		}

	} else if td.globalExclusions != nil {
		if slices.Contains(td.globalExclusions.ErrorCodes, pgErr.Code) {
			return true
		}
		if slices.Contains(td.globalExclusions.Constraints, pgErr.ConstraintName) {
			return true
		}
	}
	return false
}

func getAllArguments(row *pgcopy.Row) []any {
	var res []any
	for i := 0; i < row.Length(); i++ {
		var attrValue any
		data, err := row.GetColumn(i)
		if err != nil {
			panic(fmt.Errorf("error getting column %d: %w", i, err))
		}

		if data.IsNull {
			attrValue = nil
		} else {
			attrValue = string(data.Data)
		}
		res = append(res, attrValue)
	}
	return res
}

func isTerminationSeq(data []byte) bool {
	if data[0] == '\\' && data[1] == '.' {
		return true
	}
	return false
}

// GetRealColumns - returns only real columns (not generated)
func getRealColumns(columns []*toolkit.Column) []*toolkit.Column {
	res := make([]*toolkit.Column, 0, len(columns))
	for _, col := range columns {
		if col.IsGenerated {
			continue
		}
		res = append(res, col)
	}
	return res
}
