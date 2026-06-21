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

package dump

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

var _ core.DumpInstructionBuilder = (*DumpInstructionBuilder)(nil)

type DumpInstructionBuilder struct{}

func (b *DumpInstructionBuilder) Build(_ context.Context, cfg any) (core.DumpInstruction, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return core.DumpInstruction{}, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	return core.DumpInstruction{
		Jobs:              c.Dump.Options.Jobs,
		HeartbeatInterval: c.Common.HeartbeatInterval,
	}, nil
}
