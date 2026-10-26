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

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestTransformerDefinitions_hash_engine_required_has_engine_param(t *testing.T) {
	for name, def := range utils.DefaultTransformerRegistry.M {
		v, ok := def.Properties.GetMeta(RequireHashEngineParameter)
		if !ok || v != true {
			continue
		}
		hasEngine := slices.ContainsFunc(def.Parameters, func(p *toolkit.ParameterDefinition) bool {
			return p.Name == "engine"
		})
		require.Truef(t, hasEngine, "%s requires hash engine but has no \"engine\" parameter", name)
	}
}
