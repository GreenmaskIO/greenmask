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
	"math"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestLimiter_Limit(t *testing.T) {
	minValue := int64(math.MinInt64)
	maxValue := int64(math.MaxInt64)
	l, err := NewInt64Limiter(minValue, maxValue)
	require.NoError(t, err)
	res := l.Limit(uint64(math.MaxUint64 - 1))
	require.True(t, res == math.MaxInt64-1)
}

func TestLimiter_negative_Limit(t *testing.T) {
	minValue := int64(-10000)
	maxValue := int64(-1)
	l, err := NewInt64Limiter(minValue, maxValue)
	require.NoError(t, err)
	res := l.Limit(100000000)
	log.Debug().Int64("res", res).Msg("")
	require.True(t, res == -9999)
}
