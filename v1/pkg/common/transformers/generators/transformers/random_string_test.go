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

func TestStringTransformer_Transform_hash(t *testing.T) {
	st, err := NewRandomStringTransformer([]rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._\\-~"), 10, 100)
	require.NoError(t, err)

	hashFuncName, _, err := generators.GetHashFunctionNameBySize(st.GetRequiredGeneratorByteLength())
	require.NoError(t, err)
	g, err := generators.NewHash([]byte{}, hashFuncName)
	require.NoError(t, err)
	err = st.SetGenerator(g)
	require.NoError(t, err)
	res := st.Transform([]byte{})
	log.Debug().Str("value", string(res)).Msg("")
	require.True(t, len(res) >= 10 && len(res) <= 100)
	require.Equal(t, "-bM6BQ6~uJ", string(res))
}

func TestStringTransformer_Transform_random(t *testing.T) {
	st, err := NewRandomStringTransformer([]rune("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._\\-~"), 10, 100)
	require.NoError(t, err)

	g := generators.NewRandomBytes(0, st.GetRequiredGeneratorByteLength())
	err = st.SetGenerator(g)
	require.NoError(t, err)
	res := st.Transform([]byte{})
	log.Debug().Str("value", string(res)).Msg("")
	require.True(t, len(res) >= 10 && len(res) <= 100)
	require.Equal(t, "xvz16-K2SEYfw~rMwctQfflfq3rAHtLyyYNppFhYXrNyw027~L3TFZgxAsNxduRggmgr4sBIuMzzZOqqGiZYsOzx138AM4UGahy", string(res))
}
