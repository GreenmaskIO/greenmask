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

package generators

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestSibHashHybrid(t *testing.T) {
	expected := []byte{176, 20, 124, 157, 15, 119, 202, 213, 41, 32}
	requiredLength := 10
	sp, err := NewSipHash([]byte("test"))
	require.NoError(t, err)
	hb := NewHybridBytes(0, requiredLength, sp)
	res, err := hb.Generate([]byte("test"))
	log.Debug().
		Bytes("Res", res).
		Msg("")
	require.NoError(t, err)
	require.Equal(t, res, expected)
}
