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

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestHashTransformer_Transform(t *testing.T) {
	var attrName = "data"
	var originalValue = "old_value"
	var expectedValue = toolkit.NewValue("9n+v7qGp0ua+DgXtC9ClyjPHjWvWin6fKAmX5bZjcX4=", false)
	driver, record := getDriverAndRecord(attrName, originalValue)

	transformer, warnings, err := HashTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column": toolkit.ParamsValue(attrName),
			"salt":   toolkit.ParamsValue("12345678"),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetAttributeValueByName(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	require.Equal(t, expectedValue.Value, res.Value)

	originalValue = "123asdasdasdaasdlmaklsdmklamsdlkmalksdmlkamsdlkmalkdmlkasds"
	expectedValue = toolkit.NewValue("/nxN6Mxi8y5Ec33HUQhZPTq/nVJYNGx3uwNB61M/9SQ=", false)
	_, record = getDriverAndRecord(attrName, originalValue)
	r, err = transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err = r.GetAttributeValueByName(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	require.Equal(t, expectedValue.Value, res.Value)

}
