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

package context

import (
	"context"
	"os"
	"path"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
)

const (
	seqPrivilegesTestUser     = "seq_priv_dumper"
	seqPrivilegesTestPassword = "seq_priv_dumper_password"
	seqPrivilegesTestSequence = "seq_priv_table_id_seq"
	seqPrivilegesTestLastVal  = 100
)

// seqPrivilegesTestDb - models the "readonly role" setup from the issue report: the role may read
// every table in the schema but was never granted anything on the sequences behind them.
const seqPrivilegesTestDb = `
CREATE TABLE seq_priv_table
(
    id BIGSERIAL PRIMARY KEY,
    v  TEXT
);

INSERT INTO seq_priv_table (v) SELECT 'x' FROM generate_series(1, 100);

CREATE ROLE seq_priv_dumper LOGIN NOINHERIT PASSWORD 'seq_priv_dumper_password';
GRANT USAGE ON SCHEMA public TO seq_priv_dumper;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO seq_priv_dumper;
-- deliberately no GRANT ... ON ALL SEQUENCES IN SCHEMA public
`

// TestFindTablesAndSequences_sequenceWithoutPrivileges - regression test for a dump role that has
// SELECT on tables but no privilege on sequences. Up to PostgreSQL 17 the server raises
// "permission denied for sequence" itself, on 18 and above greenmask must detect it.
func TestFindTablesAndSequences_sequenceWithoutPrivileges(t *testing.T) {
	for _, image := range []string{"postgres:17", "postgres:18"} {
		t.Run(image, func(t *testing.T) {
			ctx := context.Background()
			connStr, cleanup, err := runPostgresContainerWithImage(ctx, image)
			require.NoError(t, err)
			defer cleanup()

			ownerCon, err := pgx.Connect(ctx, connStr)
			require.NoError(t, err)
			defer ownerCon.Close(ctx) // nolint: errcheck
			require.NoError(t, initTables(ctx, ownerCon, seqPrivilegesTestDb))

			var version int
			require.NoError(t,
				ownerCon.QueryRow(ctx, "SELECT current_setting('server_version_num')::INT").Scan(&version),
			)

			t.Run("the owner reads the real last value", func(t *testing.T) {
				tx, err := ownerCon.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(ctx) // nolint: errcheck

				_, sequences, err := findTablesAndSequences(ctx, tx, &pgdump.Options{}, version)
				require.NoError(t, err)

				idx := slices.IndexFunc(sequences, func(s *entries.Sequence) bool {
					return s.Name == seqPrivilegesTestSequence
				})
				require.NotEqual(t, -1, idx, "sequence %s not found", seqPrivilegesTestSequence)
				assert.True(t, sequences[idx].IsCalled)
				assert.EqualValues(t, seqPrivilegesTestLastVal, sequences[idx].LastValue)
			})

			unprivilegedCon := connectAs(ctx, t, connStr, seqPrivilegesTestUser, seqPrivilegesTestPassword)
			defer unprivilegedCon.Close(ctx) // nolint: errcheck

			t.Run("an unprivileged role fails the introspection", func(t *testing.T) {
				tx, err := unprivilegedCon.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(ctx) // nolint: errcheck

				// Up to PostgreSQL 17 the server error names the sequence, on 18 and above the
				// sequence list goes to the log and the error stays short
				_, _, err = findTablesAndSequences(ctx, tx, &pgdump.Options{}, version)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "permission denied for sequence")
				if version < 180000 {
					assert.Contains(t, err.Error(), seqPrivilegesTestSequence)
				}
			})

			t.Run("the runtime context fails as well", func(t *testing.T) {
				tx, err := unprivilegedCon.Begin(ctx)
				require.NoError(t, err)
				defer tx.Rollback(ctx) // nolint: errcheck

				_, err = NewRuntimeContext(
					ctx, tx, &domains.Dump{}, utils.DefaultTransformerRegistry, nil, version,
				)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "permission denied for sequence")
			})
		})
	}
}

// TestBuildTableSearchQuery_sequenceReadability - the privilege check must only be emitted for the
// versions that need it. Up to PostgreSQL 17 the query has to stay exactly as it has always been,
// since the server raises the error on its own there.
func TestBuildTableSearchQuery_sequenceReadability(t *testing.T) {
	for _, tt := range []struct {
		name          string
		version       int
		wantPrivCheck bool
	}{
		{"pg13", 130000, false},
		{"pg17", 170000, false},
		{"pg18", 180000, true},
		{"pg19", 190000, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			query, err := buildTableSearchQuery(nil, nil, nil, nil, nil, nil, tt.version)
			require.NoError(t, err)

			assert.Equal(t, tt.wantPrivCheck, strings.Contains(query, "has_sequence_privilege"))
			assert.Contains(t, query, `as "SeqReadable"`)
			// The last value expressions themselves stay identical on every version.
			assert.Equal(t, 2, strings.Count(query, "pg_sequence_last_value(c.oid::regclass)"))
		})
	}
}

// TestBuildTableSearchQuery_historicalQueryIsUntouched - on PostgreSQL 17 and older the server
// raises "permission denied for sequence" on its own, so the introspection query there must keep
// working exactly as it always has. The golden file was generated from the query as it was before
// this fix: dropping the single column the fix added has to bring the two back together, which means
// nothing else about the query changed for those versions.
//
// Comparison ignores whitespace only - it carries no meaning in SQL and the templating reflows it.
func TestBuildTableSearchQuery_historicalQueryIsUntouched(t *testing.T) {
	want, err := os.ReadFile(path.Join("testdata", "tables_and_sequences_query_historical.golden.sql"))
	require.NoError(t, err)

	got, err := buildTableSearchQuery(
		[]string{"bookings.*"},
		[]string{"booki*.boarding_pas*", "b?*.seats"},
		[]string{"bookings.flights"},
		[]string{"myserver"},
		[]string{"booki*"},
		[]string{"public*[[:digit:]]*1"},
		170000,
	)
	require.NoError(t, err)

	var kept []string
	for _, line := range strings.Split(got, "\n") {
		if strings.Contains(line, `as "SeqReadable"`) {
			continue
		}
		kept = append(kept, line)
	}
	require.Len(t, strings.Split(got, "\n"), len(kept)+1, "expected exactly one SeqReadable line")

	assert.Equal(t, normalizeSQL(string(want)), normalizeSQL(strings.Join(kept, "\n")))
}

// normalizeSQL - collapses any run of whitespace into a single space so that reindenting the query
// does not fail the comparison.
func normalizeSQL(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func connectAs(ctx context.Context, t *testing.T, connStr, user, password string) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(connStr)
	require.NoError(t, err)
	cfg.User = user
	cfg.Password = password
	con, err := pgx.ConnectConfig(ctx, cfg)
	require.NoError(t, err)
	return con
}
