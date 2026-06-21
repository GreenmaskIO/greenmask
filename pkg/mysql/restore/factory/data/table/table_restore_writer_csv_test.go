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

package table

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestCsvHandlerName(t *testing.T) {
	table := core.Table{Schema: "mydb", Name: "my_table"}
	name := csvHandlerName(table)
	assert.Equal(t, "greenmask__mydb__my_table", name)
}

func TestCsvHandlerName_SchemaAndTable(t *testing.T) {
	tests := []struct {
		schema string
		name   string
		want   string
	}{
		{"production", "orders", "greenmask__production__orders"},
		{"a", "b", "greenmask__a__b"},
		{"db-1", "table_v2", "greenmask__db-1__table_v2"},
		{"", "t", "greenmask____t"},
	}
	for _, tc := range tests {
		t.Run(tc.schema+"."+tc.name, func(t *testing.T) {
			table := core.Table{Schema: tc.schema, Name: tc.name}
			assert.Equal(t, tc.want, csvHandlerName(table))
		})
	}
}

func TestCsvHandlerName_UniquePerTable(t *testing.T) {
	t1 := core.Table{Schema: "db", Name: "users"}
	t2 := core.Table{Schema: "db", Name: "orders"}
	t3 := core.Table{Schema: "other", Name: "users"}

	n1 := csvHandlerName(t1)
	n2 := csvHandlerName(t2)
	n3 := csvHandlerName(t3)

	assert.NotEqual(t, n1, n2, "different table names must produce different handler names")
	assert.NotEqual(t, n1, n3, "different schemas must produce different handler names")
}

// TestCsvRestoreWriter_Open_wrongConnType ensures Open returns an error when
// the ConnectionConfigurer does not return a *connconfig.RestoreConnectionConfig.
func TestCsvRestoreWriter_Open_wrongConnType(t *testing.T) {
	table := testTable("db", "t", "id")
	w := NewCsvRestoreWriter(table)

	err := w.Open(context.Background(), nil, stubConnConfigurer{config: "not RestoreConnectionConfig"})
	assert.Error(t, err)
	assert.ErrorContains(t, err, "expected *connconfig.RestoreConnectionConfig")
}
