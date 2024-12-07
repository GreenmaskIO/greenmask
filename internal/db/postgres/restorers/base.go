package restorers

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/pgrestore"
	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
)

type restoreBase struct {
	opt   *pgrestore.DataSectionSettings
	entry *toc.Entry
	st    storages.Storager
}

func newRestoreBase(entry *toc.Entry, st storages.Storager, opt *pgrestore.DataSectionSettings) *restoreBase {
	return &restoreBase{
		entry: entry,
		st:    st,
		opt:   opt,
	}

}

func (rb *restoreBase) DebugInfo() string {
	return fmt.Sprintf("table %s.%s", *rb.entry.Namespace, *rb.entry.Tag)
}

func (rb *restoreBase) setSessionReplicationRole(ctx context.Context, tx pgx.Tx) error {
	if err := rb.setSuperUser(ctx, tx); err != nil {
		return fmt.Errorf("cannot set super user: %w", err)
	}
	if rb.opt.UseSessionReplicationRoleReplica {
		_, err := tx.Exec(ctx, "SET session_replication_role = 'replica'")
		if err != nil {
			return err
		}
	}
	if err := rb.resetSuperUser(ctx, tx); err != nil {
		return fmt.Errorf("cannot reset super user: %w", err)
	}
	return nil
}

func (rb *restoreBase) resetSessionReplicationRole(ctx context.Context, tx pgx.Tx) error {
	if err := rb.setSuperUser(ctx, tx); err != nil {
		return fmt.Errorf("cannot set super user: %w", err)
	}
	if rb.opt.UseSessionReplicationRoleReplica {
		_, err := tx.Exec(ctx, "RESET session_replication_role")
		if err != nil {
			return err
		}
	}
	if err := rb.resetSuperUser(ctx, tx); err != nil {
		return fmt.Errorf("cannot reset super user: %w", err)
	}
	return nil
}

func (rb *restoreBase) disableTriggers(ctx context.Context, tx pgx.Tx) error {
	if rb.opt.DisableTriggers {
		if err := rb.setSuperUser(ctx, tx); err != nil {
			return fmt.Errorf("cannot set super user: %w", err)
		}
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(
				"ALTER TABLE %s.%s DISABLE TRIGGER ALL",
				*rb.entry.Namespace,
				*rb.entry.Tag,
			),
		)
		if err != nil {
			return err
		}
		if err := rb.resetSuperUser(ctx, tx); err != nil {
			return fmt.Errorf("cannot reset super user: %w", err)
		}
	}
	return nil
}

func (rb *restoreBase) enableTriggers(ctx context.Context, tx pgx.Tx) error {
	if rb.opt.DisableTriggers {
		if err := rb.setSuperUser(ctx, tx); err != nil {
			return fmt.Errorf("cannot set super user: %w", err)
		}
		_, err := tx.Exec(
			ctx,
			fmt.Sprintf(
				"ALTER TABLE %s.%s ENABLE TRIGGER ALL",
				*rb.entry.Namespace,
				*rb.entry.Tag,
			),
		)
		if err != nil {
			return err
		}
		if err := rb.resetSuperUser(ctx, tx); err != nil {
			return fmt.Errorf("cannot reset super user: %w", err)
		}
	}
	return nil
}

func (rb *restoreBase) setSuperUser(ctx context.Context, tx pgx.Tx) error {
	if rb.opt.SuperUser != "" {
		_, err := tx.Exec(ctx, fmt.Sprintf("SET ROLE %s", rb.opt.SuperUser))
		if err != nil {
			return err
		}
	}
	return nil
}

func (rb *restoreBase) resetSuperUser(ctx context.Context, tx pgx.Tx) error {
	if rb.opt.SuperUser != "" {
		_, err := tx.Exec(ctx, "RESET ROLE")
		if err != nil {
			return err
		}
	}
	return nil
}

// setupTx - setup transaction before restore. It disables triggers and sets session replication role if set.
func (rb *restoreBase) setupTx(ctx context.Context, tx pgx.Tx) error {
	if err := rb.setSessionReplicationRole(ctx, tx); err != nil {
		return fmt.Errorf("cannot set session replication role: %w", err)
	}
	if err := rb.disableTriggers(ctx, tx); err != nil {
		return fmt.Errorf("cannot disable triggers: %w", err)
	}
	return nil
}

// resetTx - reset transaction state after restore so the changes such as temporal alter table will not be
// commited
func (rb *restoreBase) resetTx(ctx context.Context, tx pgx.Tx) error {
	if err := rb.enableTriggers(ctx, tx); err != nil {
		return fmt.Errorf("cannot enable triggers: %w", err)
	}
	if err := rb.resetSessionReplicationRole(ctx, tx); err != nil {
		return fmt.Errorf("cannot reset session replication role: %w", err)
	}
	return nil
}

// getObject returns a reader for the dump file. It warps the file in a gzip reader.
func (rb *restoreBase) getObject(ctx context.Context) (io.ReadCloser, func(), error) {
	if rb.entry.FileName == nil {
		return nil, nil, fmt.Errorf("file name in toc.Entry is empty")
	}

	r, err := rb.st.GetObject(ctx, *rb.entry.FileName)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot open dump file: %w", err)
	}
	gz, err := ioutils.GetGzipReadCloser(r, rb.opt.UsePgzip)
	if err != nil {
		if err := r.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing dump file")
		}
		return nil, nil, fmt.Errorf("cannot create gzip reader: %w", err)
	}

	closingFunc := func() {
		if err := gz.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing gzip reader")
		}
		if err := r.Close(); err != nil {
			log.Warn().
				Err(err).
				Msg("error closing dump file")
		}
	}
	return gz, closingFunc, nil
}
