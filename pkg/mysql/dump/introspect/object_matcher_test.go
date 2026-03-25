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

package introspect

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectMatcher_matchSchema(t *testing.T) {
	tests := []struct {
		name            string
		includeSchemas  []string
		excludeSchemas  []string
		schemaName      string
		expectedAllowed bool
	}{
		{
			name:            "empty lists - allow all",
			includeSchemas:  nil,
			excludeSchemas:  nil,
			schemaName:      "any_db",
			expectedAllowed: true,
		},
		{
			name:            "only include - match",
			includeSchemas:  []string{"public", "test"},
			excludeSchemas:  nil,
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "only include - no match",
			includeSchemas:  []string{"public", "test"},
			excludeSchemas:  nil,
			schemaName:      "other",
			expectedAllowed: false,
		},
		{
			name:            "only exclude - no match",
			includeSchemas:  nil,
			excludeSchemas:  []string{"mysql", "sys"},
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "only exclude - match",
			includeSchemas:  nil,
			excludeSchemas:  []string{"mysql", "sys"},
			schemaName:      "mysql",
			expectedAllowed: false,
		},
		{
			name:            "both lists - allowed",
			includeSchemas:  []string{"test_.*"},
			excludeSchemas:  []string{"test_staging"},
			schemaName:      "test_dev",
			expectedAllowed: true,
		},
		{
			name:            "both lists - excluded by blacklist",
			includeSchemas:  []string{"test_.*"},
			excludeSchemas:  []string{"test_staging"},
			schemaName:      "test_staging",
			expectedAllowed: false,
		},
		{
			name:            "both lists - excluded by whitelist",
			includeSchemas:  []string{"test_.*"},
			excludeSchemas:  []string{"test_staging"},
			schemaName:      "other_db",
			expectedAllowed: false,
		},
		{
			name:            "regex include - match",
			includeSchemas:  []string{"db_v[0-9]+"},
			schemaName:      "db_v12",
			expectedAllowed: true,
		},
		{
			name:            "literal match with dot",
			includeSchemas:  []string{"db.1"},
			schemaName:      "db.1",
			expectedAllowed: true,
		},
		{
			name:            "literal match with dot - behaves as regex (dot matches anything)",
			includeSchemas:  []string{"db.1"},
			schemaName:      "db_1",
			expectedAllowed: true, // dot is special in regex
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &optMock{}
			opt.On("GetIncludedTables").Return(nil)
			opt.On("GetExcludedTables").Return(nil)
			opt.On("GetExcludedSchemas").Return(tt.excludeSchemas)
			opt.On("GetIncludedSchemas").Return(tt.includeSchemas)
			opt.On("GetExcludedTableData").Return(nil)
			opt.On("GetIncludedTableData").Return(nil)
			opt.On("GetIncludedTableDefinitions").Return(nil)
			opt.On("GetExcludedTableDefinitions").Return(nil)
			opt.On("GetIncludedDatabases").Return(nil)
			opt.On("GetExcludedDatabases").Return(nil)

			om, err := newObjectMatcher(opt)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedAllowed, om.MatchSchemaIsAllowed(tt.schemaName))
		})
	}
}

func TestObjectMatcher_match(t *testing.T) {
	tests := []struct {
		name             string
		includeTables    []string
		excludeTables    []string
		excludeTableData []string
		includeSchemas   []string
		excludeSchemas   []string
		schemaName       string
		tableName        string
		expectedAllowed  bool
	}{
		{
			name:            "simple match",
			includeTables:   []string{"public.users"},
			schemaName:      "public",
			tableName:       "users",
			expectedAllowed: true,
		},
		{
			name:            "schema mismatch in table pattern",
			includeTables:   []string{"public.users"},
			schemaName:      "other",
			tableName:       "users",
			expectedAllowed: false,
		},
		{
			name:            "regex table match",
			includeTables:   []string{`public\.log_.*`},
			schemaName:      "public",
			tableName:       "log_2024",
			expectedAllowed: true,
		},
		{
			name:            "exclude table overrides",
			includeTables:   []string{`testdb\..*`},
			excludeTables:   []string{`testdb\.tmp_.*`},
			schemaName:      "testdb",
			tableName:       "tmp_data",
			expectedAllowed: false,
		},
		{
			name:            "schema exclusion effects table match",
			includeTables:   []string{"public.users"},
			excludeSchemas:  []string{"public"},
			schemaName:      "public",
			tableName:       "users",
			expectedAllowed: false,
		},
		{
			name:            "unnamed schema in include table list",
			includeTables:   []string{"public.users"},
			includeSchemas:  []string{"other"},
			schemaName:      "public",
			tableName:       "users",
			expectedAllowed: false,
		},
		{
			name:             "ExcludedTableData whitelisted for introspection if whitelist is active",
			includeTables:    []string{"public.t1"},
			excludeTableData: []string{"public.t2"},
			schemaName:       "public",
			tableName:        "t2",
			expectedAllowed:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opt := &optMock{}
			opt.On("GetIncludedTables").Return(tt.includeTables)
			opt.On("GetExcludedTables").Return(tt.excludeTables)
			opt.On("GetExcludedSchemas").Return(tt.excludeSchemas)
			opt.On("GetIncludedSchemas").Return(tt.includeSchemas)
			opt.On("GetExcludedTableData").Return(tt.excludeTableData)
			opt.On("GetIncludedTableData").Return(nil)
			opt.On("GetIncludedTableDefinitions").Return(nil)
			opt.On("GetExcludedTableDefinitions").Return(nil)
			opt.On("GetIncludedDatabases").Return(nil)
			opt.On("GetExcludedDatabases").Return(nil)

			om, err := newObjectMatcher(opt)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedAllowed, om.MatchNeedDumpSchema(tt.schemaName, tt.tableName) || om.MatchNeedDumpData(tt.schemaName, tt.tableName))
		})
	}
}
