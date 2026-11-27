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
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/toc"
)

func newPipeFrontendBackend(t *testing.T) (*pgproto3.Frontend, *pgproto3.Backend) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return pgproto3.NewFrontend(clientConn, clientConn), pgproto3.NewBackend(serverConn, serverConn)
}

func sendBackendMsgs(backend *pgproto3.Backend, msgs ...pgproto3.BackendMessage) error {
	for _, msg := range msgs {
		backend.Send(msg)
	}
	return backend.Flush()
}

func TestTableRestorerInitCopySuccess(t *testing.T) {
	frontend, backend := newPipeFrontendBackend(t)
	stmt := "COPY public.users FROM stdin;"
	tr := NewTableRestorer(&toc.Entry{CopyStmt: &stmt}, nil, nil)

	serverErr := make(chan error, 1)
	go func() {
		defer close(serverErr)
		msg, err := backend.Receive()
		if err != nil {
			serverErr <- err
			return
		}
		q, ok := msg.(*pgproto3.Query)
		if !ok || q.String != stmt {
			serverErr <- fmt.Errorf("unexpected frontend message %T (%v)", msg, msg)
			return
		}
		serverErr <- sendBackendMsgs(
			backend,
			&pgproto3.NoticeResponse{Message: "starting copy"},
			&pgproto3.CopyInResponse{OverallFormat: 0},
		)
	}()

	require.NoError(t, tr.initCopy(context.Background(), frontend))
	require.NoError(t, <-serverErr)
}

func TestTableRestorerInitCopyDrainsReadyForQueryAfterError(t *testing.T) {
	frontend, backend := newPipeFrontendBackend(t)
	stmt1 := "COPY public.missing FROM stdin;"
	stmt2 := "COPY public.ok FROM stdin;"
	tr1 := NewTableRestorer(&toc.Entry{CopyStmt: &stmt1}, nil, nil)
	tr2 := NewTableRestorer(&toc.Entry{CopyStmt: &stmt2}, nil, nil)

	receivedQueries := make(chan string, 2)
	serverDone := make(chan error, 1)
	go func() {
		defer close(serverDone)

		msg1, err := backend.Receive()
		if err != nil {
			serverDone <- err
			return
		}
		receivedQueries <- msg1.(*pgproto3.Query).String
		if err := sendBackendMsgs(
			backend,
			&pgproto3.ErrorResponse{
				Severity: "ERROR",
				Code:     "42P01",
				Message:  `relation "public.missing" does not exist`,
			},
			&pgproto3.NoticeResponse{Message: "trailing notice before ready"},
			&pgproto3.ReadyForQuery{TxStatus: 'E'},
		); err != nil {
			serverDone <- err
			return
		}

		msg2, err := backend.Receive()
		if err != nil {
			serverDone <- err
			return
		}
		receivedQueries <- msg2.(*pgproto3.Query).String
		serverDone <- sendBackendMsgs(backend, &pgproto3.CopyInResponse{OverallFormat: 0})
	}()

	err := tr1.initCopy(context.Background(), frontend)
	require.Error(t, err)
	require.Contains(t, err.Error(), `relation "public.missing" does not exist`)

	// Next table on the same worker connection must see CopyInResponse for stmt2,
	// not a stale ReadyForQuery left over from stmt1's ErrorResponse.
	require.NoError(t, tr2.initCopy(context.Background(), frontend))
	require.NoError(t, <-serverDone)
	require.Equal(t, stmt1, <-receivedQueries)
	require.Equal(t, stmt2, <-receivedQueries)
}

func TestTableRestorerInitCopyUnexpectedReadyForQuery(t *testing.T) {
	frontend, backend := newPipeFrontendBackend(t)
	stmt := "COPY public.users FROM stdin;"
	tr := NewTableRestorer(&toc.Entry{CopyStmt: &stmt}, nil, nil)

	serverErr := make(chan error, 1)
	go func() {
		defer close(serverErr)
		if _, err := backend.Receive(); err != nil {
			serverErr <- err
			return
		}
		serverErr <- sendBackendMsgs(backend, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	}()

	err := tr.initCopy(context.Background(), frontend)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected ReadyForQuery before CopyInResponse")
	require.NoError(t, <-serverErr)
}
