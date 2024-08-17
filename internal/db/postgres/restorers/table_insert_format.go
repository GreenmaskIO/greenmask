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

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgcopy"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
	"github.com/greenmaskio/greenmask/internal/utils/reader"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog/log"
)

type TableRestorerInsertFormat struct {
	Entry            *toc.Entry
	St               storages.Storager
	doNothing        bool
	exitOnError      bool
	query            string
	globalExclusions *domains.GlobalDataRestorationErrorExclusions
	tableExclusion   *domains.TablesDataRestorationErrorExclusions
	usePgzip         bool
}

func NewTableRestorerInsertFormat(
	entry *toc.Entry, st storages.Storager, exitOnError bool,
	doNothing bool, exclusions *domains.DataRestorationErrorExclusions,
	usePgzip bool,
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
		Entry:            entry,
		St:               st,
		exitOnError:      exitOnError,
		doNothing:        doNothing,
		globalExclusions: globalExclusion,
		tableExclusion:   tableExclusion,
		usePgzip:         usePgzip,
	}
}

func (td *TableRestorerInsertFormat) GetEntry() *toc.Entry {
	return td.Entry
}

func (td *TableRestorerInsertFormat) Execute(ctx context.Context, conn *pgx.Conn) error {

	if td.Entry.FileName == nil {
		return fmt.Errorf("cannot get file name from toc Entry")
	}

	r, err := td.St.GetObject(ctx, *td.Entry.FileName)
	if err != nil {
		return fmt.Errorf("cannot open dump file: %w", err)
	}
	defer func(reader io.ReadCloser) {
		if err := reader.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing dump file")
		}
	}(r)
	gz, err := ioutils.GetGzipReadCloser(r, td.usePgzip)
	if err != nil {
		return fmt.Errorf("cannot create gzip reader: %w", err)
	}
	defer func(gz io.Closer) {
		if err := gz.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing gzip reader")
		}
	}(gz)

	if err = td.streamInsertData(ctx, conn, gz); err != nil {
		if td.exitOnError {
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

		line, err := reader.ReadLine(buf)
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

func (td *TableRestorerInsertFormat) generateInsertStmt(row *pgcopy.Row, onConflictDoNothing bool) string {
	var placeholders []string
	for i := 0; i < row.Length(); i++ {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
	}
	var onConflict string
	if onConflictDoNothing {
		onConflict = " ON CONFLICT DO NOTHING"
	}

	res := fmt.Sprintf(
		`INSERT INTO %s.%s VALUES (%s)%s`,
		*td.Entry.Namespace,
		*td.Entry.Tag,
		strings.Join(placeholders, ", "),
		onConflict,
	)
	return res
}

func (td *TableRestorerInsertFormat) insertDataOnConflictDoNothing(
	ctx context.Context, conn *pgx.Conn, row *pgcopy.Row,
) error {
	if td.query == "" {
		td.query = td.generateInsertStmt(row, td.doNothing)
	}

	// TODO: The implementation based on pgx.Conn.Exec is not efficient for bulk inserts.
	// 	Consider rewrite to string literal that contains generated statement instead of using prepared statement
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

func (td *TableRestorerInsertFormat) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", *td.Entry.Namespace, *td.Entry.Tag)
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
