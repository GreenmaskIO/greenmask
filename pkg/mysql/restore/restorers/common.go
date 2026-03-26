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

func setupTransaction(ctx context.Context, tx *sql.Tx, disableFkChecks, disableUniqueChecks bool) error {
	if disableFkChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0;"); err != nil {
			return fmt.Errorf("disable foreign key checks: %w", err)
		}
	}

	if disableUniqueChecks {
		if _, err := tx.ExecContext(ctx, "SET UNIQUE_CHECKS=0;"); err != nil {
			return fmt.Errorf("disable unique checks: %w", err)
		}
	}
	return nil
}

func closeTransaction(ctx context.Context, tx *sql.Tx, execErr error, disableFkChecks, disableUniqueChecks bool) error {
	if tx == nil {
		return nil
	}
	if execErr != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
		}
		return nil
	}

	if disableUniqueChecks {
		if _, err := tx.ExecContext(ctx, "SET UNIQUE_CHECKS=1;"); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to enable unique checks")
		}
	}

	if disableFkChecks {
		if _, err := tx.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1;"); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to enable foreign key checks")
		}
	}

	if err := tx.Commit(); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil && rollbackErr != sql.ErrTxDone {
			log.Ctx(ctx).Error().Err(rollbackErr).Msg("failed to rollback transaction")
		}
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func closeDatabase(ctx context.Context, db *sql.DB) {
	if db != nil {
		if err := db.Close(); err != nil {
			log.Ctx(ctx).Error().Err(err).Msg("failed to close database connection")
		}
	}
}
