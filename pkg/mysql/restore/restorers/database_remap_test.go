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

package restorers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

func minimalMeta(schema, name string) core.RestorationItem {
	table := core.Table{Schema: schema, Name: name}
	return core.RestorationItem{
		ObjectDefinition: commonutils.Must(json.Marshal(table)),
	}
}

func insertMeta(schema, name string, columns ...string) core.RestorationItem {
	cols := make([]core.Column, len(columns))
	for i, c := range columns {
		cols[i] = core.Column{Name: c}
	}
	table := core.Table{Schema: schema, Name: name, Columns: cols}
	return core.RestorationItem{
		ObjectDefinition: commonutils.Must(json.Marshal(table)),
	}
}

func stubConnConfig() *mysqlmodels.ConnConfig {
	return &mysqlmodels.ConnConfig{
		Host: "127.0.0.1", Port: 3306,
		User: "root", Password: "root", Database: "test",
	}
}

func TestWithDatabaseRemap_CSV(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		remap      map[string]string
		wantSchema string
	}{
		{
			name:       "mapped schema is renamed",
			schema:     "olddb",
			remap:      map[string]string{"olddb": "newdb"},
			wantSchema: "newdb",
		},
		{
			name:       "unmapped schema is unchanged",
			schema:     "mydb",
			remap:      map[string]string{"otherdb": "newdb"},
			wantSchema: "mydb",
		},
		{
			name:       "nil remap leaves schema unchanged",
			schema:     "mydb",
			remap:      nil,
			wantSchema: "mydb",
		},
		{
			name:       "multiple entries — correct key selected",
			schema:     "db2",
			remap:      map[string]string{"db1": "tgt1", "db2": "tgt2"},
			wantSchema: "tgt2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr, err := NewTableDataRestorerCsv(
				minimalMeta(tc.schema, "tbl"),
				stubConnConfig(), nil, nil,
				WithDatabaseRemap(tc.remap),
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSchema, rr.Meta()[core.MetaKeyTableSchema])
		})
	}
}

func TestWithDatabaseRemap_Insert(t *testing.T) {
	tests := []struct {
		name       string
		schema     string
		remap      map[string]string
		wantSchema string
	}{
		{
			name:       "mapped schema is renamed",
			schema:     "srcdb",
			remap:      map[string]string{"srcdb": "tgtdb"},
			wantSchema: "tgtdb",
		},
		{
			name:       "unmapped schema is unchanged",
			schema:     "mydb",
			remap:      map[string]string{"other": "tgt"},
			wantSchema: "mydb",
		},
		{
			name:       "nil remap leaves schema unchanged",
			schema:     "mydb",
			remap:      nil,
			wantSchema: "mydb",
		},
		{
			name:       "multiple entries — correct key selected",
			schema:     "db1",
			remap:      map[string]string{"db1": "tgt1", "db2": "tgt2"},
			wantSchema: "tgt1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rr, err := NewTableDataRestorerInsert(
				insertMeta(tc.schema, "users", "id"),
				stubConnConfig(), nil, nil,
				WithDatabaseRemap(tc.remap),
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantSchema, rr.Meta()[core.MetaKeyTableSchema])
		})
	}
}
