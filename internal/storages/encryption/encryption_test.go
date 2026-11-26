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
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"filippo.io/age"
	"github.com/greenmaskio/storages"
	"github.com/greenmaskio/storages/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/domains"
)

const ageHeader = "age-encryption.org/v1"

func newKey(t *testing.T) *age.X25519Identity {
	t.Helper()
	id, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	return id
}

func cfgFor(id *age.X25519Identity) domains.EncryptionConfig {
	if id == nil {
		return domains.EncryptionConfig{}
	}
	return domains.EncryptionConfig{Key: id.String()}
}

// open wraps root for cfgFor(id) and returns the dump dumpId.
func open(t *testing.T, root storages.Storager, id *age.X25519Identity, dumpId string) storages.Storager {
	t.Helper()
	st, err := NewStorage(root, cfgFor(id), "heartbeat")
	require.NoError(t, err)
	dump, err := st.SubStorage(dumpId, true)
	require.NoError(t, err)
	return dump
}

func put(t *testing.T, st storages.Storager, name, data string) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), name, strings.NewReader(data)))
}

func get(t *testing.T, st storages.Storager, name string) string {
	t.Helper()
	r, err := st.GetObject(context.Background(), name)
	require.NoError(t, err)
	defer func() { require.NoError(t, r.Close()) }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(data)
}

func raw(t *testing.T, root storages.Storager, dumpId string) storages.Storager {
	t.Helper()
	dir, err := root.SubStorage(dumpId, true)
	require.NoError(t, err)
	return dir
}

// writeEncrypted writes a finished dump 100 encrypted to id into a new root.
func writeEncrypted(t *testing.T, id *age.X25519Identity) storages.Storager {
	t.Helper()
	root := inmemory.New("")
	dump := open(t, root, id, "100")
	put(t, dump, "heartbeat", "done")
	put(t, dump, "metadata.json", `{"a":1}`)
	return root
}

func TestParse(t *testing.T) {
	id := newKey(t)
	tests := []struct {
		name    string
		cfg     domains.EncryptionConfig
		wantErr bool
		want    *age.X25519Identity
	}{
		{name: "no key", cfg: domains.EncryptionConfig{}},
		{name: "key", cfg: cfgFor(id), want: id},
		{
			name: "key with surrounding whitespace",
			cfg:  domains.EncryptionConfig{Key: " \t" + id.String() + "\n"},
			want: id,
		},
		{name: "garbage key", cfg: domains.EncryptionConfig{Key: "AGE-SECRET-KEY-1GARBAGE"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			identity, err := parse(tt.cfg)
			require.Equal(t, err, Validate(tt.cfg))
			_, newErr := NewStorage(inmemory.New(""), tt.cfg)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrInvalidKey)
				require.ErrorIs(t, newErr, ErrInvalidKey)
				assert.NotContains(t, err.Error(), tt.cfg.Key)
				return
			}
			require.NoError(t, err)
			require.NoError(t, newErr)
			if tt.want == nil {
				assert.Nil(t, identity)
				return
			}
			assert.Equal(t, tt.want.String(), identity.String())
		})
	}
}

func TestPlainDump(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)
	for name, key := range map[string]*age.X25519Identity{"no key": nil, "with key": id} {
		t.Run(name, func(t *testing.T) {
			root := inmemory.New("")
			put(t, raw(t, root, "100"), "metadata.json", `{"a":1}`)
			dump := open(t, root, key, "100")
			assert.Equal(t, `{"a":1}`, get(t, dump, "metadata.json"))
			encrypted, _, err := Inspect(ctx, dump)
			require.NoError(t, err)
			assert.False(t, encrypted)
		})
	}

	t.Run("without a key a new dump is plain", func(t *testing.T) {
		root := inmemory.New("")
		put(t, open(t, root, nil, "100"), "metadata.json", `{"a":1}`)
		assert.Equal(t, `{"a":1}`, get(t, raw(t, root, "100"), "metadata.json"))
		ok, err := raw(t, root, "100").Exists(ctx, MarkerFileName)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestEncryptedDump(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)
	other := newKey(t)
	root := writeEncrypted(t, id)
	dir := raw(t, root, "100")

	t.Run("layout", func(t *testing.T) {
		assert.JSONEq(t, `{"recipient":"`+id.Recipient().String()+`"}`, get(t, dir, MarkerFileName))
		assert.Equal(t, "done", get(t, dir, "heartbeat"), "the heartbeat stays plain")
		assert.True(t, strings.HasPrefix(get(t, dir, "metadata.json"), ageHeader))
	})

	t.Run("with the key", func(t *testing.T) {
		dump := open(t, root, id, "100")
		assert.Equal(t, `{"a":1}`, get(t, dump, "metadata.json"))
		encrypted, _, err := Inspect(ctx, dump)
		require.NoError(t, err)
		assert.True(t, encrypted)
	})

	t.Run("another key", func(t *testing.T) {
		dump := open(t, root, other, "100")
		_, err := dump.GetObject(ctx, "metadata.json")
		require.ErrorIs(t, err, ErrWrongKey)
		assert.Contains(t, err.Error(), id.Recipient().String())
		_, err = dump.GetObjectRange(ctx, "metadata.json", 0, 1)
		require.ErrorIs(t, err, ErrWrongKey)

		assert.Equal(t, "done", get(t, dump, "heartbeat"), "the heartbeat needs no key")
		ok, err := dump.Exists(ctx, "metadata.json")
		require.NoError(t, err)
		assert.True(t, ok, "names need no key")
		encrypted, _, err := Inspect(ctx, dump)
		require.NoError(t, err)
		assert.True(t, encrypted)
	})

	t.Run("without a key the storage is not wrapped", func(t *testing.T) {
		st, err := NewStorage(root, cfgFor(nil), "heartbeat")
		require.NoError(t, err)
		assert.Same(t, root, st)

		dump := open(t, root, nil, "100")
		assert.True(t, strings.HasPrefix(get(t, dump, "metadata.json"), ageHeader), "the ciphertext is read as it is")
		assert.Equal(t, "done", get(t, dump, "heartbeat"))
		encrypted, _, err := Inspect(ctx, dump)
		require.NoError(t, err)
		assert.True(t, encrypted)
	})

	t.Run("writing with another key fails", func(t *testing.T) {
		root := writeEncrypted(t, id)
		err := open(t, root, other, "100").PutObject(ctx, "toc.dat", strings.NewReader("x"))
		require.ErrorIs(t, err, ErrWrongKey)
	})

	t.Run("a plaintext object of an encrypted dump fails to read", func(t *testing.T) {
		root := writeEncrypted(t, id)
		put(t, raw(t, root, "100"), "toc.dat", "plain")
		_, err := open(t, root, id, "100").GetObject(ctx, "toc.dat")
		require.Error(t, err)
	})

	broken := map[string]string{"empty marker": "{}", "not json": ageHeader}
	for name, body := range broken {
		t.Run(name, func(t *testing.T) {
			root := inmemory.New("")
			put(t, raw(t, root, "100"), MarkerFileName, body)
			_, err := open(t, root, id, "100").GetObject(ctx, "metadata.json")
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrWrongKey)
		})
	}
}

func TestInspect(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)
	encRoot := writeEncrypted(t, id)
	plainRoot := inmemory.New("")
	put(t, raw(t, plainRoot, "100"), "metadata.json", `{"a":1}`)

	tests := []struct {
		name      string
		root      storages.Storager
		key       *age.X25519Identity
		encrypted bool
		readable  bool
	}{
		{name: "plain without a key", root: plainRoot, readable: true},
		{name: "plain with a key", root: plainRoot, key: id, readable: true},
		{name: "encrypted with its key", root: encRoot, key: id, encrypted: true, readable: true},
		{name: "encrypted with another key", root: encRoot, key: newKey(t), encrypted: true},
		{name: "encrypted without a key", root: encRoot, encrypted: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encrypted, readable, err := Inspect(ctx, open(t, tt.root, tt.key, "100"))
			require.NoError(t, err)
			assert.Equal(t, tt.encrypted, encrypted)
			assert.Equal(t, tt.readable, readable)
		})
	}

	t.Run("a broken marker is an error", func(t *testing.T) {
		root := inmemory.New("")
		put(t, raw(t, root, "100"), MarkerFileName, "{}")
		_, _, err := Inspect(ctx, open(t, root, id, "100"))
		require.Error(t, err)
	})
}

func TestPlainFilesArePassedIn(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)
	root := inmemory.New("")
	st, err := NewStorage(root, cfgFor(id))
	require.NoError(t, err)
	dump, err := st.SubStorage("100", true)
	require.NoError(t, err)
	put(t, dump, "metadata.json", `{"a":1}`)
	put(t, dump, "heartbeat", "done")

	assert.True(t, strings.HasPrefix(get(t, raw(t, root, "100"), "heartbeat"), ageHeader),
		"without plain files, the heartbeat is encrypted like any object")
	ok, err := raw(t, root, "100").Exists(ctx, MarkerFileName)
	require.NoError(t, err)
	assert.True(t, ok, "the marker stays plain")
	assert.JSONEq(t, `{"recipient":"`+id.Recipient().String()+`"}`, get(t, raw(t, root, "100"), MarkerFileName))
}

func TestStat(t *testing.T) {
	id := newKey(t)
	root := writeEncrypted(t, id)
	dump := open(t, root, id, "100")

	stored, err := raw(t, root, "100").Stat("metadata.json")
	require.NoError(t, err)

	stat, err := dump.Stat("metadata.json")
	require.NoError(t, err)
	assert.True(t, stat.Exist)
	assert.Equal(t, stored.Size, stat.Size, "Stat reports the stored object")

	assert.Equal(t, `{"a":1}`, get(t, dump, "metadata.json"))
	stat, err = dump.Stat("metadata.json")
	require.NoError(t, err)
	assert.Equal(t, stored.Size, stat.Size, "reading the object does not change Stat")

	stat, err = dump.Stat("heartbeat")
	require.NoError(t, err)
	assert.EqualValues(t, len("done"), stat.Size)
}

// countingStorage counts marker writes and fails the first failMarker of them.
type countingStorage struct {
	storages.Storager
	markerPuts atomic.Int32
	failMarker atomic.Int32
}

func (s *countingStorage) PutObject(ctx context.Context, filePath string, body io.Reader) error {
	if filePath == MarkerFileName {
		if s.failMarker.Load() > 0 {
			s.failMarker.Add(-1)
			return errors.New("marker write failed")
		}
		s.markerPuts.Add(1)
	}
	return s.Storager.PutObject(ctx, filePath, body)
}

func newCountingDump(t *testing.T, id *age.X25519Identity) (*countingStorage, storages.Storager) {
	t.Helper()
	dir := &countingStorage{Storager: inmemory.New("")}
	return dir, newDumpStorage(dir, id, map[string]bool{MarkerFileName: true, "heartbeat": true})
}

func TestWrite(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)

	t.Run("marker is written before the first encrypted object", func(t *testing.T) {
		root := inmemory.New("")
		dump := open(t, root, id, "100")
		put(t, dump, "heartbeat", "in-progress")
		ok, err := raw(t, root, "100").Exists(ctx, MarkerFileName)
		require.NoError(t, err)
		assert.False(t, ok, "a heartbeat alone writes no marker")

		put(t, dump, "metadata.json", `{"a":1}`)
		ok, err = raw(t, root, "100").Exists(ctx, MarkerFileName)
		require.NoError(t, err)
		assert.True(t, ok)
	})

	t.Run("concurrent writes put the marker once", func(t *testing.T) {
		dir, dump := newCountingDump(t, id)
		var wg sync.WaitGroup
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				name := string(rune('a'+i)) + ".dat"
				assert.NoError(t, dump.PutObject(ctx, name, bytes.NewReader([]byte(name))))
			}()
		}
		wg.Wait()
		assert.EqualValues(t, 1, dir.markerPuts.Load())
	})

	t.Run("failed marker write is retried", func(t *testing.T) {
		dir, dump := newCountingDump(t, id)
		dir.failMarker.Store(1)
		require.Error(t, dump.PutObject(ctx, "a.dat", strings.NewReader("a")))
		ok, err := dir.Exists(ctx, "a.dat")
		require.NoError(t, err)
		assert.False(t, ok, "object is not written without its marker")

		put(t, dump, "a.dat", "a")
		assert.EqualValues(t, 1, dir.markerPuts.Load())
	})

	t.Run("first write through a sub-directory marks the dump root", func(t *testing.T) {
		root := inmemory.New("")
		sub, err := open(t, root, id, "100").SubStorage("blobs", true)
		require.NoError(t, err)
		put(t, sub, "heartbeat", "blob")
		assert.JSONEq(t, `{"recipient":"`+id.Recipient().String()+`"}`, get(t, raw(t, root, "100"), MarkerFileName))

		rawSub, err := raw(t, root, "100").SubStorage("blobs", true)
		require.NoError(t, err)
		assert.True(t, strings.HasPrefix(get(t, rawSub, "heartbeat"), ageHeader), "only the dump root has plain files")

		readSub, err := open(t, root, id, "100").SubStorage("blobs", true)
		require.NoError(t, err)
		assert.Equal(t, "blob", get(t, readSub, "heartbeat"))
	})

	t.Run("encrypting into a plain dump fails", func(t *testing.T) {
		root := inmemory.New("")
		put(t, raw(t, root, "100"), "metadata.json", `{"a":1}`)
		err := open(t, root, id, "100").PutObject(ctx, "toc.dat", strings.NewReader("x"))
		require.ErrorIs(t, err, ErrPlainDump)
		ok, err := raw(t, root, "100").Exists(ctx, MarkerFileName)
		require.NoError(t, err)
		assert.False(t, ok)
	})
}

func TestRoot(t *testing.T) {
	ctx := context.Background()
	id := newKey(t)
	root := writeEncrypted(t, id)
	st, err := NewStorage(root, cfgFor(id), "heartbeat")
	require.NoError(t, err)

	_, err = st.GetObject(ctx, "100/metadata.json")
	require.ErrorIs(t, err, ErrRootObject)
	_, err = st.GetObjectRange(ctx, "100/metadata.json", 0, 1)
	require.ErrorIs(t, err, ErrRootObject)
	require.ErrorIs(t, st.PutObject(ctx, "100/x", strings.NewReader("x")), ErrRootObject)

	ok, err := st.Exists(ctx, "100/metadata.json")
	require.NoError(t, err)
	assert.True(t, ok)

	_, dirs, err := st.ListDir(ctx)
	require.NoError(t, err)
	require.Len(t, dirs, 1)
	assert.Equal(t, "100", dirs[0].Dirname())
	assert.Equal(t, `{"a":1}`, get(t, dirs[0], "metadata.json"), "listed dumps are decrypted")

	require.NoError(t, st.DeleteAll(ctx, "100"))
	_, dirs, err = st.ListDir(ctx)
	require.NoError(t, err)
	assert.Empty(t, dirs)
}
