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
	"github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/stretchr/testify/require"
)

func TestChoiceTransformer_Transform(t *testing.T) {
	data := []*toolkit.RawValue{
		{Data: []byte("a")},
		{Data: []byte("b")},
	}
	tr := NewRandomChoiceTransformer(data)
	g, err := generators.NewHash([]byte{}, "sha1")
	require.NoError(t, err)
	g = generators.NewHashReducer(g, tr.GetRequiredGeneratorByteLength())
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform([]byte{})
	require.NoError(t, err)
	require.Contains(t, data, res)
}
