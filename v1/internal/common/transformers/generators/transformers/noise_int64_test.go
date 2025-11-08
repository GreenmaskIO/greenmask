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
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
)

func TestNoiseInt64Transformer_Transform(t *testing.T) {
	minVal := int64(-1000)
	maxVal := int64(100)
	l, err := NewNoiseInt64Limiter(minVal, maxVal)
	require.NoError(t, err)
	tr, err := NewNoiseInt64Transformer(l, 0.1, 0.9)
	require.NoError(t, err)
	g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform(nil, 17)
	require.NoError(t, err)
	log.Debug().Int64("value", res).Msg("")
	require.True(t, res >= minVal && res <= maxVal)
}
