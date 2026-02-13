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

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestNewFloat64Transformer(t *testing.T) {
	limiter, err := NewFloat64Limiter(-1, 1, 2)
	require.NoError(t, err)
	tr := NewRandomFloat64Transformer(limiter)
	g, err := generators.NewHash([]byte{}, "sha1")
	require.NoError(t, err)
	g = generators.NewHashReducer(g, tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform(nil, []byte{})
	require.NoError(t, err)
	log.Debug().Msgf("value = %f", res)
	require.True(t, res >= -1 && res <= 1)
}
