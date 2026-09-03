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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConnConfig(t *testing.T) {
	t.Run("attaches the notice handler", func(t *testing.T) {
		cfg, err := newConnConfig("postgres://user@localhost:5432/db")
		require.NoError(t, err)
		require.NotNil(t, cfg.OnNotice, "connections must carry a notice handler")

		// The handler is the logging one: invoking it must produce a log entry.
		buf := captureLog(t, zerolog.DebugLevel)

		cfg.OnNotice(nil, &pgconn.Notice{
			SeverityUnlocalized: "NOTICE", Message: "handler wired",
		})
		assert.Contains(t, buf.String(), "handler wired")
	})

	t.Run("reports an unparsable dsn", func(t *testing.T) {
		_, err := newConnConfig("://not a dsn")
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

// TestNoDirectPgxConnect guards the invariant that makes notice logging
// reliable: a connection opened without our handler silently discards notices,
// so this package must not reach for pgx's constructors directly. Use openConn.
//
// connect.go is the one exception - it is where the handler is attached.
func TestNoDirectPgxConnect(t *testing.T) {
	const connectHelperFile = "connect.go"
	forbidden := map[string]bool{"Connect": true, "ConnectConfig": true}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || name == connectHelperFile {
			continue
		}

		file, err := parser.ParseFile(fset, name, nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok || !forbidden[sel.Sel.Name] {
				return true
			}
			ident, ok := sel.X.(*ast.Ident)
			if ok && ident.Name == "pgx" {
				t.Errorf(
					"%s:%d: use openConn instead of pgx.%s - a connection "+
						"opened without our handler discards PostgreSQL notices",
					name, fset.Position(sel.Pos()).Line, sel.Sel.Name,
				)
			}
			return true
		})
	}
}
