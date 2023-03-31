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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestSetNullTransformer_Transform(t *testing.T) {
	var columnName = "id"
	var originalValue = "1"
	var expectedValue = "\\N"

	driver, record := getDriverAndRecord(columnName, originalValue)

	transformer, warnings, err := SetNullTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column": toolkit.ParamsValue(columnName),
		},
		nil,
	)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	encoded, err := r.Encode()
	require.NoError(t, err)
	res, err := encoded.Encode()
	require.NoError(t, err)
	assert.Equal(t, expectedValue, string(res))
}
