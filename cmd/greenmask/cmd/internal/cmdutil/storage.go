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

// Package cmdutil holds the setup the CLI commands share.
package cmdutil

import (
	"context"

	"github.com/greenmaskio/storages"
	"github.com/rs/zerolog/log"

	cmdInternals "github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/storages/encryption"
)

// OpenStorage builds the storage of dumps and exits on failure. With an encryption key, the
// storage is wrapped for client-side encryption; without one, it is the storage as it is.
func OpenStorage(ctx context.Context, cfg *domains.Config) storages.Storager {
	// A bad key fails before the storage is built, so it never connects.
	if err := encryption.Validate(cfg.Storage.Encryption); err != nil {
		log.Ctx(ctx).
			Info().
			Msg("set storage.encryption.key (STORAGE_ENCRYPTION_KEY) to an age identity, AGE-SECRET-KEY-1...")
		log.Ctx(ctx).Fatal().Err(err).Msg("invalid encryption config")
	}
	root, err := builder.GetStorage(ctx, &cfg.Storage, &cfg.Log)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("cannot build storage")
	}
	st, err := encryption.NewStorage(root, cfg.Storage.Encryption, cmdInternals.HeartBeatFileName)
	if err != nil {
		log.Ctx(ctx).Fatal().Err(err).Msg("cannot build storage")
	}
	return st
}
