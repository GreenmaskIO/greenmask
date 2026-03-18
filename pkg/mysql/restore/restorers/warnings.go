// Copyright 2025 Greenmask
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
	"context"
	"database/sql"
	"fmt"

	"github.com/rs/zerolog/log"
)

func showWarnings(ctx context.Context, db *sql.DB, printWarnings bool, maxFetch int) error {
	totalCount, err := getWarningCount(ctx, db)
	if err != nil {
		return fmt.Errorf("get warning count: %w", err)
	}
	if totalCount == 0 {
		return nil
	}

	if !printWarnings {
		log.Ctx(ctx).Warn().Int("count", totalCount).Msg("warnings occurred during table data restoration")
		return nil
	}

	var query string
	if maxFetch > 0 {
		query = fmt.Sprintf("SHOW WARNINGS LIMIT %d", maxFetch)
	} else {
		query = "SHOW WARNINGS"
	}
	query += ";"

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to close show warnings rows")
		}
	}()

	var count int
	for rows.Next() {
		var level, code, message string
		if err := rows.Scan(&level, &code, &message); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		log.Ctx(ctx).Warn().
			Str("MysqlLevel", level).
			Str("MysqlCode", code).
			Str("MysqlWarning", message).
			Msg("warning from Mysql server after restoring table data")
		count++
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if maxFetch > 0 && totalCount > maxFetch {
		log.Ctx(ctx).Warn().Int("suppressedCount", totalCount-maxFetch).Msg("more warnings suppressed")
	}

	return nil
}

func showInsertWarnings(ctx context.Context, db *sql.DB, printWarnings bool, maxFetch int, batchNum int, printedCount *int) (int, error) {
	totalCount, err := getWarningCount(ctx, db)
	if err != nil {
		return 0, fmt.Errorf("get warning count: %w", err)
	}
	if totalCount == 0 {
		return 0, nil
	}

	if !printWarnings {
		return totalCount, nil
	}

	var fetchLimit int
	if maxFetch > 0 {
		fetchLimit = maxFetch - *printedCount
		if fetchLimit <= 0 {
			return totalCount, nil
		}
	}

	var query string
	if fetchLimit > 0 {
		query = fmt.Sprintf("SHOW WARNINGS LIMIT %d", fetchLimit)
	} else {
		query = "SHOW WARNINGS"
	}
	query += ";"

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("execute query: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to close show warnings rows")
		}
	}()

	for rows.Next() {
		var level, code, message string
		if err := rows.Scan(&level, &code, &message); err != nil {
			return 0, fmt.Errorf("scan row: %w", err)
		}

		log.Ctx(ctx).Warn().
			Str("MysqlLevel", level).
			Str("MysqlCode", code).
			Str("MysqlWarning", message).
			Int("BatchNum", batchNum).
			Msg("warning from Mysql server after restoring table data")
		*printedCount++
	}

	if err := rows.Err(); err != nil {
		return 0, err
	}

	return totalCount, nil
}

func getWarningCount(ctx context.Context, db *sql.DB) (int, error) {
	var count int
	if err := db.QueryRowContext(ctx, "SHOW COUNT(*) WARNINGS;").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
