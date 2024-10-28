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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestRealAddressTransformer_Transform(t *testing.T) {
	driver, record := getDriverAndRecord("data", "somaval")

	columns := []*RealAddressColumn{
		{
			Name:     "data",
			Template: "{{ .Address }} {{ .City }} {{ .State }} {{ .PostalCode }} {{ .Latitude }} {{ .Longitude }}",
		},
	}

	rawData, err := json.Marshal(columns)
	require.NoError(t, err)

	transformer, warnings, err := RealAddressTransformerDefinition.Instance(
		context.Background(),
		driver,
		map[string]toolkit.ParamsValue{
			"columns": rawData,
		},
		nil,
		"",
	)

	require.NoError(t, err)
	require.Empty(t, warnings)

	_, err = transformer.Transformer.Transform(context.Background(), record)
	require.NoError(t, err)
	rawValue, err := record.GetRawColumnValueByName("data")
	require.NoError(t, err)
	require.False(t, rawValue.IsNull)
	require.Regexp(t, `.* \d+ \-?\d+.\d+ \-?\d+.\d+`, string(rawValue.Data))
}

func TestMakeNewFakeTransformerFunction_parsing_error(t *testing.T) {
	driver, _ := getDriverAndRecord("data", "somaval")

	columns := []*RealAddressColumn{
		{
			Name:     "data",
			Template: "{{ .Address }",
		},
	}

	rawData, err := json.Marshal(columns)
	require.NoError(t, err)

	_, warnings, err := RealAddressTransformerDefinition.Instance(
		context.Background(),
		driver,
		map[string]toolkit.ParamsValue{
			"columns": rawData,
		},
		nil,
		"",
	)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, "error parsing template", warnings[0].Msg)
}

func TestMakeNewFakeTransformerFunction_validation_error(t *testing.T) {
	driver, _ := getDriverAndRecord("data", "somaval")

	columns := []*RealAddressColumn{
		{
			Name:     "data",
			Template: "{{ .Address1 }}",
		},
	}

	rawData, err := json.Marshal(columns)
	require.NoError(t, err)

	_, warnings, err := RealAddressTransformerDefinition.Instance(
		context.Background(),
		driver,
		map[string]toolkit.ParamsValue{
			"columns": rawData,
		},
		nil,
		"",
	)
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	require.Equal(t, "error validating template", warnings[0].Msg)
}
