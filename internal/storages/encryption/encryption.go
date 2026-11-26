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

// Package encryption wraps the storage of dumps so that each dump is encrypted with age on the
// client or left plain; callers use the wrapped storage like any other.
package encryption

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"filippo.io/age"
	"github.com/greenmaskio/storages"

	"github.com/greenmaskio/greenmask/internal/domains"
)

var (
	ErrInvalidKey = errors.New("invalid encryption key")

	// ErrWrongKey is a dump encrypted to a key other than the config's.
	ErrWrongKey = errors.New("dump is encrypted to another key")

	// ErrPlainDump is an encrypted write to a dump that already holds plain objects.
	ErrPlainDump = errors.New("cannot encrypt into a plain dump")
	// ErrRootObject is an object read or written at the storage root, outside any dump.
	ErrRootObject = errors.New("objects at the storage root are not supported")
)

// parse reads the config's key, nil without one; the key's text never reaches the error.
func parse(cfg domains.EncryptionConfig) (*age.X25519Identity, error) {
	key := strings.TrimSpace(cfg.Key)
	if key == "" {
		return nil, nil
	}
	identity, err := age.ParseX25519Identity(key)
	if err != nil {
		return nil, fmt.Errorf("%w: not an age X25519 identity", ErrInvalidKey)
	}
	return identity, nil
}

// Validate reports an invalid key in the config.
func Validate(cfg domains.EncryptionConfig) error {
	_, err := parse(cfg)
	return err
}

// NewStorage wraps root, whose directories are dumps; plainFiles name the objects at a dump's root
// stored as plaintext, as the marker always is. Without a key, it returns root as it is.
func NewStorage(
	root storages.Storager, cfg domains.EncryptionConfig, plainFiles ...string,
) (storages.Storager, error) {
	identity, err := parse(cfg)
	if err != nil {
		return nil, err
	}
	if identity == nil {
		return root, nil
	}
	plain := map[string]bool{MarkerFileName: true}
	for _, name := range plainFiles {
		plain[name] = true
	}
	return &rootStorage{Storager: root, identity: identity, plain: plain}, nil
}

// Inspect reports whether the dump in dir is encrypted and whether its objects can be read: a
// plain dump always, an encrypted one only with its key.
func Inspect(ctx context.Context, dir storages.Storager) (encrypted, readable bool, err error) {
	d, ok := dir.(*dumpStorage)
	if !ok {
		encrypted, err = dir.Exists(ctx, MarkerFileName)
		return encrypted, !encrypted, err
	}
	m, err := d.state.resolve(ctx)
	// A locked dump comes with its error; only an unresolved mode is a failure here.
	if m == modeUnknown {
		return false, false, err
	}
	return m != modePlain, m != modeLocked, nil
}
