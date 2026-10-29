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

package transformers

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestTransformerDefinitions_column_nullable(t *testing.T) {
	tests := []struct {
		def      *utils.TransformerDefinition
		nullable bool
	}{
		// Can write NULL via "\N" in values
		{def: DictTransformerDefinition, nullable: true},
		{def: ChoiceTransformerDefinition, nullable: true},
		// Never writes NULL
		{def: HashedPasswordTransformerDefinition, nullable: false},
	}
	for _, tt := range tests {
		t.Run(tt.def.Properties.Name, func(t *testing.T) {
			idx := slices.IndexFunc(tt.def.Parameters, func(p *toolkit.ParameterDefinition) bool {
				return p.Name == "column"
			})
			require.NotEqual(t, -1, idx)
			assert.Equal(t, tt.nullable, tt.def.Parameters[idx].ColumnProperties.Nullable)
		})
	}
}
