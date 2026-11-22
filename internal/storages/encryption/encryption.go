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
	"github.com/greenmaskio/storages/encrypted"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
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

// keys is the parsed encryption config; identity is nil without a key.
type keys struct {
	identity *age.X25519Identity
}

// parse reads the config's key; the key's text never reaches the error.
func parse(cfg domains.EncryptionConfig) (*keys, error) {
	key := strings.TrimSpace(cfg.Key)
	if key == "" {
		return &keys{}, nil
	}
	identity, err := age.ParseX25519Identity(key)
	if err != nil {
		return nil, fmt.Errorf("%w: not an age X25519 identity", ErrInvalidKey)
	}
	return &keys{identity: identity}, nil
}

func (k *keys) recipient() string {
	if k.identity == nil {
		return ""
	}
	return k.identity.Recipient().String()
}

// lockErr is the error of reading a dump encrypted to recipient, nil when the key opens it.
func (k *keys) lockErr(recipient string) error {
	if recipient != k.recipient() {
		return fmt.Errorf("%w: %s", ErrWrongKey, recipient)
	}
	return nil
}

func (k *keys) wrap(st storages.Storager) (storages.Storager, error) {
	enc, err := encrypted.New(st, k.identity, encrypted.WithLogger(logger.NewSlog("storages")))
	if err != nil {
		return nil, fmt.Errorf("open encrypted storage: %w", err)
	}
	return enc, nil
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
	k, err := parse(cfg)
	if err != nil {
		return nil, err
	}
	if k.identity == nil {
		return root, nil
	}
	plain := map[string]bool{MarkerFileName: true}
	for _, name := range plainFiles {
		plain[name] = true
	}
	return &rootStorage{Storager: root, keys: k, plain: plain}, nil
}

// IsEncrypted reports whether the dump in dir is encrypted; it needs no key.
func IsEncrypted(ctx context.Context, dir storages.Storager) (bool, error) {
	return dir.Exists(ctx, MarkerFileName)
}

// Readable reports whether the objects of the dump in dir can be read: a plain dump always, an
// encrypted one only with its key.
func Readable(ctx context.Context, dir storages.Storager) (bool, error) {
	if d, ok := dir.(*dumpStorage); ok {
		m, err := d.state.resolve(ctx)
		if m == modeLocked {
			return false, nil
		}
		return err == nil, err
	}
	encrypted, err := IsEncrypted(ctx, dir)
	return !encrypted, err
}
