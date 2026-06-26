// Copyright 2026 Greenmask
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

package ssh

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/pkg/sftp"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

// errStorageClosed is returned by Client() once the storage has been closed.
var errStorageClosed = errors.New("ssh storage is closed")

// SFTPClient is the subset of *sftp.Client used by the storage. It is kept as
// an interface so the dependency surface stays small and explicit.
type SFTPClient interface {
	ReadDir(path string) ([]os.FileInfo, error)
	Join(elem ...string) string
	Remove(path string) error
	RemoveDirectory(path string) error
	Stat(p string) (os.FileInfo, error)
	Open(path string) (*sftp.File, error)
	Create(path string) (*sftp.File, error)
	MkdirAll(path string) error
}

// sftpLazy establishes the SSH/SFTP connection on the first Client() call and
// reuses it in all subsequent calls (shared across every SubStorage clone).
// Close() releases the connection; it must be called by the lifecycle owner
// (e.g. a long-running server) — otherwise the connection, its receive
// goroutine and its socket leak until the process exits.
type sftpLazy struct {
	address string
	config  *ssh.ClientConfig

	mu        sync.Mutex
	sftp      SFTPClient
	ssh       *ssh.Client
	connErr   error
	attempted bool
	closed    bool
}

func newSFTPLazy(addr string, config *ssh.ClientConfig) *sftpLazy {
	return &sftpLazy{
		address: addr,
		config:  config,
	}
}

func (l *sftpLazy) Client(ctx context.Context) (SFTPClient, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed {
		return nil, errStorageClosed
	}
	// Connect only on the first call; reuse the connection afterwards. The
	// outcome (success or failure) is cached for the lifetime of this storage.
	if l.attempted {
		return l.sftp, l.connErr
	}
	l.attempted = true

	sftpClient, sshClient, err := connect(ctx, l.address, l.config)
	if err != nil {
		l.connErr = fmt.Errorf("lazy SSH connection error: %w", err)
		return nil, l.connErr
	}
	l.sftp = sftpClient
	l.ssh = sshClient
	return l.sftp, nil
}

// Close releases the underlying connection. It is idempotent and safe to call
// before the connection has been established. Closing the SSH client tears down
// the TCP connection, which also unblocks and stops the sftp receive goroutine;
// we deliberately close the SSH client rather than sftp.Client.Close(), which
// can hang on a wedged connection because it waits on that receive loop.
func (l *sftpLazy) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.closed = true
	if l.ssh == nil {
		return nil // never connected (or already torn down)
	}
	err := l.ssh.Close()
	l.ssh = nil
	l.sftp = nil
	return err
}

func connect(ctx context.Context, addr string, config *ssh.ClientConfig) (*sftp.Client, *ssh.Client, error) {
	sshClient, err := ssh.Dial("tcp", addr, config)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to %s via SSH: %w", addr, err)
	}

	sftpClient, err := sftp.NewClient(sshClient)
	if err != nil {
		// The SSH client owns the TCP connection; close it so a failed SFTP
		// handshake does not leak the socket. A close error here only matters
		// for diagnostics — surface it via the context logger, but still return
		// the original (more useful) SFTP error to the caller.
		if cerr := sshClient.Close(); cerr != nil {
			log.Ctx(ctx).Warn().Err(cerr).Str("address", addr).
				Msg("error closing SSH client after failed SFTP handshake")
		}
		return nil, nil, fmt.Errorf("failed to connect to %s via SFTP: %w", addr, err)
	}

	return sftpClient, sshClient, nil
}
