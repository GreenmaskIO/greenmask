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
