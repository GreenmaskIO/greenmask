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

package transformers

import (
	"testing"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestUuidTransformer_Transform_hash(t *testing.T) {
	regexp := `^[\d\w]{8}-[\d\w]{4}-[\d\w]{4}-[\d\w]{4}-[\d\w]{12}$`

	ut := NewRandomUuidTransformer()
	hashFuncName, _, err := generators.GetHashFunctionNameBySize(ut.GetRequiredGeneratorByteLength())
	require.NoError(t, err)
	g, err := generators.NewHash([]byte{}, hashFuncName)
	require.NoError(t, err)
	g = generators.NewHashReducer(g, uuidTransformerRequiredLength)
	err = ut.SetGenerator(g)
	require.NoError(t, err)
	res, err := ut.Transform([]byte{})
	require.NoError(t, err)
	resStr, err := res.MarshalText()
	require.NoError(t, err)
	require.Regexp(t, regexp, string(resStr))
	log.Debug().Str("value", string(resStr)).Msg("")
}
