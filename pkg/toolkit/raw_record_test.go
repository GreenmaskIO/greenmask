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

package toolkit

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawRecordDto(t *testing.T) {
	rawData := []byte(`{"8":{"d":"","n":true},"9":{"d":"","n":true}}`)
	expected := []byte(`{"8":{"d":"test","n":false},"9":{"d":"","n":true}}`)
	rrd := &RawRecordStr{}
	err := json.Unmarshal(rawData, rrd)
	require.NoError(t, err)

	err = rrd.SetColumn(8, NewRawValue([]byte("test"), false))
	require.NoError(t, err)
	_, err = rrd.GetColumn(10)
	require.Error(t, err)

	res, err := json.Marshal(rrd)
	require.NoError(t, err)
	require.JSONEq(t, string(expected), string(res))
}

func TestRawRecordCsv_null_and_bounds(t *testing.T) {
	rr := NewRawRecordCsv(2, nil)
	require.NoError(t, rr.SetColumn(0, NewRawValue([]byte("x"), false)))
	require.NoError(t, rr.SetColumn(1, NewRawValue(nil, true)))
	v, err := rr.GetColumn(1)
	require.NoError(t, err)
	require.True(t, v.IsNull)

	require.Error(t, rr.SetColumn(2, NewRawValue(nil, true)))
	_, err = rr.GetColumn(2)
	require.Error(t, err)

	// a decoded record may have fewer fields than expected
	rr.Data = []string{"x"}
	_, err = rr.GetColumn(1)
	require.Error(t, err)
}

func TestRawRecordCsv_Encode_single_empty_field(t *testing.T) {
	rr := NewRawRecordCsv(1, nil)
	require.NoError(t, rr.SetColumn(0, NewRawValue([]byte(""), false)))
	data, err := rr.Encode()
	require.NoError(t, err)
	require.Equal(t, "\"\"\n", string(data))
}

func TestRawRecordText_null(t *testing.T) {
	rr := NewRawRecordText()
	require.NoError(t, rr.Decode([]byte("\\N")))
	v, err := rr.GetColumn(0)
	require.NoError(t, err)
	require.True(t, v.IsNull)
}

func TestJsonRecordWithAttrNames_null_without_data(t *testing.T) {
	columns := []*Column{{Name: "data", Idx: 0}}
	text := NewJsonRecordWithAttrNamesText(columns)
	require.NoError(t, json.Unmarshal([]byte(`{"data":{"n":true}}`), text))
	v, err := text.GetColumn(0)
	require.NoError(t, err)
	require.True(t, v.IsNull)

	require.NoError(t, json.Unmarshal([]byte(`{"data":null}`), text))
	_, err = text.GetColumn(0)
	require.Error(t, err)

	binary := NewJsonRecordWithAttrNamesBinary(columns)
	require.NoError(t, binary.Decode([]byte(`{"data":{"d":"eA==","n":false}}`)))
	v, err = binary.GetColumn(0)
	require.NoError(t, err)
	require.Equal(t, "x", string(v.Data))
}
