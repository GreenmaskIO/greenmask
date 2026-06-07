// Copyright 2025 Greenmask
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

package restore

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

// Restore is the PostgreSQL restore orchestrator placeholder.
// Run returns "not implemented yet" until the PostgreSQL port is complete.
type Restore struct {
	cfg    *config.Config
	st     core.Storager
	dumpID core.DumpID
}

// New returns a Restore for use as an engines.Restorer.
func New(cfg *config.Config, st core.Storager, dumpID core.DumpID) (*Restore, error) {
	return &Restore{cfg: cfg, st: st, dumpID: dumpID}, nil
}

func (r *Restore) Run(_ context.Context) error {
	return fmt.Errorf("postgresql restore: not implemented yet")
}
