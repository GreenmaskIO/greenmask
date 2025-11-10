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

	"github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/registry"
	"github.com/greenmaskio/greenmask/v1/internal/config"
)

func RunDump(ctx context.Context, cfg *config.Config, st interfaces.Storager, opts ...Option) error {
	dump, err := NewDump(cfg, registry.DefaultTransformerRegistry, st, opts...)
	if err != nil {
		return fmt.Errorf("init dump process: %w", err)
	}
	if err := dump.Run(ctx); err != nil {
		return fmt.Errorf("run dump process: %w", err)
	}
	return nil
}
