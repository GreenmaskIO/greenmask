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

package filterconfig

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
)

var _ core.FilterConfigBuilder = (*Builder)(nil)

// Builder is the common, DBMS-agnostic implementation of core.FilterConfigBuilder.
// It withdraws the include/exclude filtering options from config.Config and
// initializes a core.FilterConfig with them.
type Builder struct{}

// New returns a common FilterConfigBuilder.
func New() *Builder {
	return &Builder{}
}

func (b *Builder) Build(cfg any) (core.FilterConfig, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return core.FilterConfig{}, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	opts := c.Dump.Options
	return core.FilterConfig{
		IncludeDatabase:        opts.IncludeDatabase,
		ExcludeDatabase:        opts.ExcludeDatabase,
		IncludeSchema:          opts.IncludeSchema,
		ExcludeSchema:          opts.ExcludeSchema,
		IncludeTableData:       opts.IncludeTableData,
		ExcludeTableData:       opts.ExcludeTableData,
		IncludeTable:           opts.IncludeTable,
		ExcludeTable:           opts.ExcludeTable,
		IncludeTableDefinition: opts.IncludeTableDefinition,
		ExcludeTableDefinition: opts.ExcludeTableDefinition,
		DataOnly:               opts.DataOnly,
		SchemaOnly:             opts.SchemaOnly,
	}, nil
}
