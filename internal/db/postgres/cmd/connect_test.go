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

package cmd

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnConfig(t *testing.T) {
	t.Run("attaches the notice handler", func(t *testing.T) {
		var got *pgconn.Notice
		cfg, err := newConnConfig("postgres://user@localhost:5432/db", func(_ *pgconn.PgConn, n *pgconn.Notice) {
			got = n
		})
		require.NoError(t, err)
		require.NotNil(t, cfg.OnNotice, "connections must carry a notice handler")

		cfg.OnNotice(nil, &pgconn.Notice{Message: "handler wired"})
		require.NotNil(t, got)
		assert.Equal(t, "handler wired", got.Message)
	})

	t.Run("reports an unparsable dsn", func(t *testing.T) {
		_, err := newConnConfig("://not a dsn", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot parse connection string")
	})
}

func TestOpenConn(t *testing.T) {
	t.Run("propagates a dsn parse failure without dialling", func(t *testing.T) {
		conn, err := openConn(context.Background(), "://not a dsn")
		require.Error(t, err)
		assert.Nil(t, conn)
		assert.Contains(t, err.Error(), "cannot parse connection string")
	})
}
