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
	var expectedValue = toolkit.NewValue("jzTVGK2UHz3ERhrYiZDoDzcKeMxSsgxHHgWlL9OrkZ4=", false)
	driver, record := getDriverAndRecord(attrName, originalValue)

	transformer, warnings, err := HashTransformerDefinition.Instance(
		context.Background(),
		driver, map[string]toolkit.ParamsValue{
			"column": toolkit.ParamsValue(attrName),
			"salt":   toolkit.ParamsValue("MTIzNDU2Nw=="),
		},
		nil,
	)
	require.NoError(t, err)
	require.Empty(t, warnings)

	r, err := transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err := r.GetColumnValueByName(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	require.Equal(t, expectedValue.Value, res.Value)

	originalValue = "123asdasdasdaasdlmaklsdmklamsdlkmalksdmlkamsdlkmalkdmlkasds"
	expectedValue = toolkit.NewValue("kZsJbWbVoBGMqniHTCzU6fJrxQdlfeqhYIUxOo3JniA=", false)
	_, record = getDriverAndRecord(attrName, originalValue)
	r, err = transformer.Transformer.Transform(
		context.Background(),
		record,
	)
	require.NoError(t, err)
	res, err = r.GetColumnValueByName(attrName)
	require.NoError(t, err)

	require.Equal(t, expectedValue.IsNull, res.IsNull)
	require.Equal(t, expectedValue.Value, res.Value)

}
