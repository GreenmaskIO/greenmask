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

package encryption

import (
	"context"
	"fmt"
	"io"
	"sync"

	"filippo.io/age"
	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/encrypted"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

// rootStorage is the storage of dumps; it hands out its directories wrapped as dumps.
type rootStorage struct {
	storages.Storager
	identity *age.X25519Identity
	plain    map[string]bool
}

func (s *rootStorage) SubStorage(subPath string, relative bool) (storages.Storager, error) {
	sub, err := s.Storager.SubStorage(subPath, relative)
	if err != nil {
		return nil, err
	}
	return newDumpStorage(sub, s.identity, s.plain), nil
}

func (s *rootStorage) ListDir(ctx context.Context) ([]string, []storages.Storager, error) {
	files, dirs, err := s.Storager.ListDir(ctx)
	for i, dir := range dirs {
		dirs[i] = newDumpStorage(dir, s.identity, s.plain)
	}
	return files, dirs, err
}

// GetObject, GetObjectRange and PutObject refuse a path like "id/file", which would bypass the
// dump's encryption.
func (s *rootStorage) GetObject(context.Context, string) (io.ReadCloser, error) {
	return nil, ErrRootObject
}

func (s *rootStorage) GetObjectRange(context.Context, string, int64, int64) (io.ReadCloser, error) {
	return nil, ErrRootObject
}

func (s *rootStorage) PutObject(context.Context, string, io.Reader) error {
	return ErrRootObject
}

type mode int

const (
	modeUnknown mode = iota
	modePlain
	modeEncrypted
	modeLocked
)

// dumpState is the encryption mode of one dump, shared by its directory and sub-directories.
// It is read from the marker on the first object read or write.
type dumpState struct {
	root     storages.Storager
	identity *age.X25519Identity
	plain    map[string]bool
	mx       sync.Mutex
	mode     mode
	lockErr  error
}

// resolve reads the marker once; a failed read is retried on the next call.
func (d *dumpState) resolve(ctx context.Context) (mode, error) {
	d.mx.Lock()
	defer d.mx.Unlock()
	return d.resolveLocked(ctx)
}

func (d *dumpState) resolveLocked(ctx context.Context) (mode, error) {
	if d.mode != modeUnknown {
		return d.mode, d.lockErr
	}
	marker, found, err := getMarker(ctx, d.root)
	if err != nil {
		return modeUnknown, err
	}
	switch {
	case !found:
		d.mode = modePlain
	case marker.Recipient != d.identity.Recipient().String():
		d.mode, d.lockErr = modeLocked, fmt.Errorf("%w: %s", ErrWrongKey, marker.Recipient)
	default:
		d.mode = modeEncrypted
	}
	return d.mode, d.lockErr
}

// resolveWrite returns the mode a write uses. A dump without a marker becomes encrypted when it
// holds no plain object yet; the marker is written before the first encrypted object.
func (d *dumpState) resolveWrite(ctx context.Context) (mode, error) {
	d.mx.Lock()
	defer d.mx.Unlock()
	m, err := d.resolveLocked(ctx)
	if err != nil || m != modePlain {
		return m, err
	}
	files, dirs, err := d.root.ListDir(ctx)
	if err != nil {
		return modeUnknown, err
	}
	for _, f := range files {
		if !d.plain[f] {
			return modeUnknown, ErrPlainDump
		}
	}
	if len(dirs) > 0 {
		return modeUnknown, ErrPlainDump
	}
	if err = putMarker(ctx, d.root, Marker{Recipient: d.identity.Recipient().String()}); err != nil {
		return modeUnknown, err
	}
	d.mode = modeEncrypted
	return d.mode, nil
}

// dumpStorage is a dump directory, or a sub-directory of one. At the dump root, the plain files
// stay plain; every other object follows the dump's mode.
type dumpStorage struct {
	storages.Storager
	state *dumpState
	sub   bool

	encOnce sync.Once
	enc     storages.Storager
	encErr  error
}

func newDumpStorage(dir storages.Storager, identity *age.X25519Identity, plain map[string]bool) *dumpStorage {
	return &dumpStorage{
		Storager: dir,
		state: &dumpState{
			root:     dir,
			identity: identity,
			plain:    plain,
		},
	}
}

func (s *dumpStorage) isPlain(name string) bool {
	return !s.sub && s.state.plain[name]
}

func (s *dumpStorage) encrypted() (storages.Storager, error) {
	s.encOnce.Do(func() {
		s.enc, s.encErr = encrypted.New(
			s.Storager, s.state.identity, encrypted.WithLogger(logger.NewSlog("storages")),
		)
		if s.encErr != nil {
			s.encErr = fmt.Errorf("open encrypted storage: %w", s.encErr)
		}
	})
	return s.enc, s.encErr
}

// target returns the storage an object of mode m is read from or written to.
func (s *dumpStorage) target(m mode, err error) (storages.Storager, error) {
	if err != nil {
		return nil, err
	}
	if m == modeEncrypted {
		return s.encrypted()
	}
	return s.Storager, nil
}

func (s *dumpStorage) reader(ctx context.Context, name string) (storages.Storager, error) {
	if s.isPlain(name) {
		return s.Storager, nil
	}
	return s.target(s.state.resolve(ctx))
}

func (s *dumpStorage) GetObject(ctx context.Context, filePath string) (io.ReadCloser, error) {
	st, err := s.reader(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return st.GetObject(ctx, filePath)
}

func (s *dumpStorage) GetObjectRange(
	ctx context.Context, filePath string, offset, length int64,
) (io.ReadCloser, error) {
	st, err := s.reader(ctx, filePath)
	if err != nil {
		return nil, err
	}
	return st.GetObjectRange(ctx, filePath, offset, length)
}

func (s *dumpStorage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	if s.isPlain(filePath) {
		return s.Storager.PutObject(ctx, filePath, body)
	}
	st, err := s.target(s.state.resolveWrite(ctx))
	if err != nil {
		return err
	}
	return st.PutObject(ctx, filePath, body)
}

func (s *dumpStorage) SubStorage(subPath string, relative bool) (storages.Storager, error) {
	sub, err := s.Storager.SubStorage(subPath, relative)
	if err != nil {
		return nil, err
	}
	return &dumpStorage{Storager: sub, state: s.state, sub: true}, nil
}

func (s *dumpStorage) ListDir(ctx context.Context) ([]string, []storages.Storager, error) {
	files, dirs, err := s.Storager.ListDir(ctx)
	for i, dir := range dirs {
		dirs[i] = &dumpStorage{Storager: dir, state: s.state, sub: true}
	}
	return files, dirs, err
}
