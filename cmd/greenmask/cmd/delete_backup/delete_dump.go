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

package delete_backup

import (
	"context"
	"fmt"
	"slices"

	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"

	pgDomains "github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	Cmd = &cobra.Command{
		Use:   "delete",
		Short: "delete dump from the storage with a specific ID",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			if err := logger.SetLogLevel(Config.Log.Level, Config.Log.Format); err != nil {
				log.Fatal().Err(err).Msg("")
			}

			if err := deleteDump(args[0]); err != nil {
				log.Fatal().Err(err).Msg("")
			}
		},
	}
	Config = pgDomains.NewConfig()
)

func deleteDump(dumpId string) error {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := builder.GetStorage(ctx, &Config.Storage, &Config.Log)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}

	if !slices.ContainsFunc(dirs, func(sst storages.Storager) bool {
		return dumpId == sst.Dirname()
	}) {
		return fmt.Errorf("dump with id %s was not found", dumpId)
	}

	if err = st.DeleteAll(ctx, dumpId); err != nil {
		return fmt.Errorf("storage error: %s", err)
	}

	return nil
}
