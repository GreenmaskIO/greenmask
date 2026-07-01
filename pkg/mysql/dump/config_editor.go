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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.ConfigEditor = (*ConfigEditor)(nil)

// ConfigEditor enriches user configuration. The stub passes the config through
// unchanged so downstream stages observe the explicit user configuration.
type ConfigEditor struct{}

func (s *ConfigEditor) EditConfig(ctx context.Context, input core.ConfigEditInput) []core.TableConfig {
	return input.Config
}
