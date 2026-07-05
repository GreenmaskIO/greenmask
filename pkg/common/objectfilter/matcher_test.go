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

package objectfilter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var mysqlSystemSchemas = []string{"information_schema", "mysql", "performance_schema", "sys"}

func TestMatcher_schemaAllowed(t *testing.T) {
	tests := []struct {
		name            string
		includeSchemas  []string
		excludeSchemas  []string
		includeDatabase []string
		excludeDatabase []string
		systemSchemas   []string
		schemaName      string
		expectedAllowed bool
	}{
		{
			name:            "empty lists - allow all",
			schemaName:      "any_db",
			expectedAllowed: true,
		},
		{
			name:            "only include - match",
			includeSchemas:  []string{"public", "test"},
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "only include - no match",
			includeSchemas:  []string{"public", "test"},
			schemaName:      "other",
			expectedAllowed: false,
		},
		{
			name:            "only exclude - no match",
			excludeSchemas:  []string{"mysql", "sys"},
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "only exclude - match",
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
			name:            "exact literal include - match",
			includeSchemas:  []string{"public"},
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "exact literal include - anchored, no partial match",
			includeSchemas:  []string{"pub"},
			schemaName:      "public",
			expectedAllowed: false,
		},
		{
			name:            "dot is a regex wildcard, not a literal dot",
			includeSchemas:  []string{"db.1"},
			schemaName:      "db_1",
			expectedAllowed: true,
		},
		{
			name:            "system schema excluded by default",
			systemSchemas:   mysqlSystemSchemas,
			schemaName:      "information_schema",
			expectedAllowed: false,
		},
		{
			name:            "system schema explicitly included overrides default exclusion",
			includeSchemas:  []string{"mysql"},
			systemSchemas:   mysqlSystemSchemas,
			schemaName:      "mysql",
			expectedAllowed: true,
		},
		{
			name:            "non-system schema allowed even with system schemas configured",
			systemSchemas:   mysqlSystemSchemas,
			schemaName:      "public",
			expectedAllowed: true,
		},
		{
			name:            "database include list folds into schema matching",
			includeDatabase: []string{"shop"},
			schemaName:      "shop",
			expectedAllowed: true,
		},
		{
			name:            "database include list excludes non-listed schema",
			includeDatabase: []string{"shop"},
			schemaName:      "warehouse",
			expectedAllowed: false,
		},
		{
			name:            "database exclude list folds into schema matching",
			excludeDatabase: []string{"warehouse"},
			schemaName:      "warehouse",
			expectedAllowed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newMatcher(core.FilterConfig{
				IncludeSchema:   tt.includeSchemas,
				ExcludeSchema:   tt.excludeSchemas,
				IncludeDatabase: tt.includeDatabase,
				ExcludeDatabase: tt.excludeDatabase,
			}, tt.systemSchemas)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedAllowed, m.schemaAllowed(tt.schemaName))
		})
	}
}

func TestMatcher_isAllowed(t *testing.T) {
	tests := []struct {
		name             string
		cfg              core.FilterConfig
		systemSchemas    []string
		schemaName       string
		tableName        string
		expectedAllowed  bool
		expectDumpSchema bool
		expectDumpData   bool
	}{
		{
			name:             "no filters - allow all",
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   true,
		},
		{
			name:             "include table - match",
			cfg:              core.FilterConfig{IncludeTable: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   true,
		},
		{
			name:             "include table - exact literal match",
			cfg:              core.FilterConfig{IncludeTable: []string{`public\.users`}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   true,
		},
		{
			name:             "include table - anchored, prefix does not match",
			cfg:              core.FilterConfig{IncludeTable: []string{`public\.user`}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "include table - anchored, suffix does not match",
			cfg:              core.FilterConfig{IncludeTable: []string{`public\.users`}},
			schemaName:       "public",
			tableName:        "users_archive",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "include table - schema mismatch",
			cfg:              core.FilterConfig{IncludeTable: []string{"public.users"}},
			schemaName:       "other",
			tableName:        "users",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "include table - other table not in list",
			cfg:              core.FilterConfig{IncludeTable: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "orders",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "regex table match",
			cfg:              core.FilterConfig{IncludeTable: []string{`public\.log_.*`}},
			schemaName:       "public",
			tableName:        "log_2024",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   true,
		},
		{
			name: "exclude table overrides include",
			cfg: core.FilterConfig{
				IncludeTable: []string{`testdb\..*`},
				ExcludeTable: []string{`testdb\.tmp_.*`},
			},
			schemaName:       "testdb",
			tableName:        "tmp_data",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "schema exclusion overrides table include",
			cfg:              core.FilterConfig{IncludeTable: []string{"public.users"}, ExcludeSchema: []string{"public"}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
		{
			name:             "include table definition only - schema dumped, data not",
			cfg:              core.FilterConfig{IncludeTableDefinition: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   false,
		},
		{
			name:             "exclude table definition - schema not dumped",
			cfg:              core.FilterConfig{ExcludeTableDefinition: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: false,
			expectDumpData:   true,
		},
		{
			name:             "exclude table data - data not dumped, schema still dumped",
			cfg:              core.FilterConfig{ExcludeTableData: []string{"public.audit"}},
			schemaName:       "public",
			tableName:        "audit",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   false,
		},
		{
			name:             "include table data only - data dumped, others excluded",
			cfg:              core.FilterConfig{IncludeTableData: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "users",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   true,
		},
		{
			name:             "include table data only - non-listed table data excluded but schema dumped",
			cfg:              core.FilterConfig{IncludeTableData: []string{"public.users"}},
			schemaName:       "public",
			tableName:        "orders",
			expectedAllowed:  true,
			expectDumpSchema: true,
			expectDumpData:   false,
		},
		{
			name:             "system schema table fully excluded",
			cfg:              core.FilterConfig{},
			systemSchemas:    mysqlSystemSchemas,
			schemaName:       "mysql",
			tableName:        "user",
			expectedAllowed:  false,
			expectDumpSchema: false,
			expectDumpData:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := newMatcher(tt.cfg, tt.systemSchemas)
			require.NoError(t, err)
			assert.Equal(t, tt.expectDumpSchema, m.needDumpSchema(tt.schemaName, tt.tableName), "needDumpSchema")
			assert.Equal(t, tt.expectDumpData, m.needDumpData(tt.schemaName, tt.tableName), "needDumpData")
			assert.Equal(t, tt.expectedAllowed, m.isAllowed(tt.schemaName, tt.tableName), "isAllowed")
		})
	}
}

func TestNewMatcher_invalidRegexp(t *testing.T) {
	fields := []struct {
		name string
		cfg  core.FilterConfig
	}{
		{"include table", core.FilterConfig{IncludeTable: []string{"("}}},
		{"exclude table", core.FilterConfig{ExcludeTable: []string{"("}}},
		{"include table definition", core.FilterConfig{IncludeTableDefinition: []string{"("}}},
		{"exclude table definition", core.FilterConfig{ExcludeTableDefinition: []string{"("}}},
		{"include table data", core.FilterConfig{IncludeTableData: []string{"("}}},
		{"exclude table data", core.FilterConfig{ExcludeTableData: []string{"("}}},
		{"include schema", core.FilterConfig{IncludeSchema: []string{"("}}},
		{"exclude schema", core.FilterConfig{ExcludeSchema: []string{"("}}},
		{"include database", core.FilterConfig{IncludeDatabase: []string{"("}}},
		{"exclude database", core.FilterConfig{ExcludeDatabase: []string{"("}}},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			_, err := newMatcher(f.cfg, nil)
			require.Error(t, err)
		})
	}
}

func TestCompileRegexps_anchored(t *testing.T) {
	res, err := compileRegexps([]string{"users"})
	require.NoError(t, err)
	require.Len(t, res, 1)
	// Patterns are anchored: an exact match succeeds, a substring does not.
	assert.True(t, res[0].MatchString("users"))
	assert.False(t, res[0].MatchString("users_archive"))
	assert.False(t, res[0].MatchString("public_users"))
}
