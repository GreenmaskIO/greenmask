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

// serveReplies answers each received query message with the next set of replies.
func serveReplies(backend *pgproto3.Backend, replies ...[]pgproto3.BackendMessage) error {
	for _, msgs := range replies {
		for {
			msg, err := backend.Receive()
			if err != nil {
				return err
			}
			if _, ok := msg.(*pgproto3.Query); ok || len(msgs) > 0 && isCopyDoneExpected(msg) {
				break
			}
		}
		for _, msg := range msgs {
			backend.Send(msg)
		}
		if err := backend.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func isCopyDoneExpected(msg pgproto3.FrontendMessage) bool {
	_, ok := msg.(*pgproto3.CopyDone)
	return ok
}

func TestTableRestorer_initCopy(t *testing.T) {
	tests := []struct {
		name    string
		replies []pgproto3.BackendMessage
		wantErr string
	}{
		{
			name: "copy starts after a notice",
			replies: []pgproto3.BackendMessage{
				&pgproto3.NoticeResponse{Message: "starting copy"},
				&pgproto3.CopyInResponse{},
			},
		},
		{
			name: "error is returned after ReadyForQuery",
			replies: []pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P01", Message: "relation does not exist"},
				&pgproto3.NoticeResponse{Message: "trailing notice"},
				&pgproto3.ReadyForQuery{TxStatus: 'E'},
			},
			wantErr: "relation does not exist",
		},
		{
			name: "statement is not a copy",
			replies: []pgproto3.BackendMessage{
				&pgproto3.EmptyQueryResponse{},
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			},
			wantErr: "unknown message",
		},
		{
			name: "ReadyForQuery without CopyInResponse",
			replies: []pgproto3.BackendMessage{
				&pgproto3.ReadyForQuery{TxStatus: 'I'},
			},
			wantErr: "unexpected ReadyForQuery before CopyInResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frontend, backend := newPipeFrontendBackend(t)
			stmt := "COPY public.users FROM stdin;"
			tr := NewTableRestorer(&toc.Entry{CopyStmt: &stmt}, nil, nil)

			replies := [][]pgproto3.BackendMessage{tt.replies}
			if tt.wantErr != "" {
				// Next statement on the same connection must still receive its CopyInResponse cleanly.
				replies = append(replies, []pgproto3.BackendMessage{&pgproto3.CopyInResponse{}})
			}
			serverErr := make(chan error, 1)
			go func() { serverErr <- serveReplies(backend, replies...) }()

			err := tr.initCopy(context.Background(), frontend)
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
				require.NoError(t, tr.initCopy(context.Background(), frontend))
			}
			require.NoError(t, <-serverErr)
		})
	}
}

func TestTableRestorer_completeBatch_failedCopyStart(t *testing.T) {
	frontend, backend := newPipeFrontendBackend(t)
	stmt := "COPY public.users FROM stdin;"
	tr := NewTableRestorer(&toc.Entry{CopyStmt: &stmt}, nil, nil)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- serveReplies(
			backend,
			// 1. CopyDone for finishing the previous batch
			[]pgproto3.BackendMessage{
				&pgproto3.CommandComplete{CommandTag: []byte("COPY 1")},
				&pgproto3.ReadyForQuery{TxStatus: 'T'},
			},
			// 2. Next batch's COPY fails in initCopy and drains through ReadyForQuery
			[]pgproto3.BackendMessage{
				&pgproto3.ErrorResponse{Severity: "ERROR", Code: "42P01", Message: "relation does not exist"},
				&pgproto3.ReadyForQuery{TxStatus: 'E'},
			},
			// 3. Subsequent operation (e.g. ROLLBACK / next table COPY) sees a synchronized stream
			[]pgproto3.BackendMessage{
				&pgproto3.CopyInResponse{},
			},
		)
	}()

	err := tr.completeBatch(context.Background(), frontend)
	require.ErrorContains(t, err, "relation does not exist")
	require.NoError(t, tr.initCopy(context.Background(), frontend))
	require.NoError(t, <-serverErr)
}
