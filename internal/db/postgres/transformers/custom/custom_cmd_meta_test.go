// Copyright 2026 Greenmask
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

package custom

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestCmdTransformer_getMetadata_timezone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)

	tests := []struct {
		name     string
		opts     []toolkit.DriverOption
		expected string
	}{
		{name: "default", expected: "UTC"},
		{name: "Europe/Berlin", opts: []toolkit.DriverOption{toolkit.WithTimestamptzLocation(berlin)}, expected: "Europe/Berlin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &toolkit.Table{
				Schema: "public",
				Name:   "test",
				Oid:    1224,
				Columns: []*toolkit.Column{
					{Name: "ts", TypeName: "timestamptz", TypeOid: pgtype.TimestamptzOID, Num: 1, Length: -1},
				},
			}
			driver, _, err := toolkit.NewDriver(table, nil, tt.opts...)
			require.NoError(t, err)
			ct := &CmdTransformer{driver: driver, parameters: map[string]toolkit.Parameterizer{}}

			data, err := ct.getMetadata()
			require.NoError(t, err)
			meta := &toolkit.Meta{}
			require.NoError(t, json.Unmarshal(data, meta))
			assert.Equal(t, tt.expected, meta.Timezone)
		})
	}
}
