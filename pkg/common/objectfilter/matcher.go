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
	"fmt"
	"regexp"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// matcher applies the DBMS-agnostic include/exclude filter rules carried by
// core.FilterConfig to schema-qualified object identities. It is a generic port
// of the per-DBMS object matchers: every pattern list is compiled to anchored
// regexps and matched against either the schema name or the fully-qualified
// "schema.name" of a relation.
type matcher struct {
	includeTables           []*regexp.Regexp
	excludeTables           []*regexp.Regexp
	includeTableDefinitions []*regexp.Regexp
	excludeTableDefinitions []*regexp.Regexp
	includeTableData        []*regexp.Regexp
	excludeTableData        []*regexp.Regexp
	includeSchemas          []*regexp.Regexp
	excludeSchemas          []*regexp.Regexp
	// systemSchemas are excluded by default unless explicitly included; the list
	// is DBMS-specific and supplied by the concrete ObjectFilter.
	systemSchemas []string
}

// newMatcher builds a matcher from the filter config. Database-scope patterns are
// folded into the schema patterns: at the object level only schema names are
// available, and for engines where a database is a schema (e.g. MySQL) the two
// lists address the same namespace.
func newMatcher(cfg core.FilterConfig, systemSchemas []string) (*matcher, error) {
	includeSchemaPatterns := append(append([]string{}, cfg.IncludeSchema...), cfg.IncludeDatabase...)
	excludeSchemaPatterns := append(append([]string{}, cfg.ExcludeSchema...), cfg.ExcludeDatabase...)

	specs := []struct {
		name     string
		patterns []string
		dst      *[]*regexp.Regexp
	}{
		{"include tables", cfg.IncludeTable, new([]*regexp.Regexp)},
		{"exclude tables", cfg.ExcludeTable, new([]*regexp.Regexp)},
		{"include table definitions", cfg.IncludeTableDefinition, new([]*regexp.Regexp)},
		{"exclude table definitions", cfg.ExcludeTableDefinition, new([]*regexp.Regexp)},
		{"include table data", cfg.IncludeTableData, new([]*regexp.Regexp)},
		{"exclude table data", cfg.ExcludeTableData, new([]*regexp.Regexp)},
		{"include schemas", includeSchemaPatterns, new([]*regexp.Regexp)},
		{"exclude schemas", excludeSchemaPatterns, new([]*regexp.Regexp)},
	}
	for _, s := range specs {
		compiled, err := compileRegexps(s.patterns)
		if err != nil {
			return nil, fmt.Errorf("compile %s regexps: %w", s.name, err)
		}
		*s.dst = compiled
	}

	return &matcher{
		includeTables:           *specs[0].dst,
		excludeTables:           *specs[1].dst,
		includeTableDefinitions: *specs[2].dst,
		excludeTableDefinitions: *specs[3].dst,
		includeTableData:        *specs[4].dst,
		excludeTableData:        *specs[5].dst,
		includeSchemas:          *specs[6].dst,
		excludeSchemas:          *specs[7].dst,
		systemSchemas:           systemSchemas,
	}, nil
}

// isAllowed reports whether an object participates in the dump in any way —
// either its definition or its data needs to be dumped.
func (m *matcher) isAllowed(schemaName, tableName string) bool {
	return m.needDumpSchema(schemaName, tableName) || m.needDumpData(schemaName, tableName)
}

func (m *matcher) needDumpSchema(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !m.schemaAllowed(schemaName) {
		return false
	}

	if matchAny(m.excludeTables, fullTableName) || matchAny(m.excludeTableDefinitions, fullTableName) {
		return false
	}

	if len(m.includeTables) > 0 || len(m.includeTableDefinitions) > 0 {
		return matchAny(m.includeTables, fullTableName) || matchAny(m.includeTableDefinitions, fullTableName)
	}

	return true
}

func (m *matcher) needDumpData(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !m.schemaAllowed(schemaName) {
		return false
	}

	if m.isTableDataExcluded(schemaName, tableName) {
		return false
	}

	// Explicit data inclusions take precedence for the data section.
	if len(m.includeTableData) > 0 {
		return matchAny(m.includeTableData, fullTableName)
	}

	// Table inclusions cover both schema and data.
	if len(m.includeTables) > 0 {
		return matchAny(m.includeTables, fullTableName)
	}

	// Definitions explicitly included but not the tables themselves means
	// schema-only for those objects.
	if len(m.includeTableDefinitions) > 0 {
		return false
	}

	return true
}

func (m *matcher) isTableDataExcluded(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !m.schemaAllowed(schemaName) {
		return false
	}
	return matchAny(m.excludeTableData, fullTableName) || matchAny(m.excludeTables, fullTableName)
}

func (m *matcher) schemaAllowed(schemaName string) bool {
	return m.isSchemaIncluded(schemaName) && !m.isSchemaExcluded(schemaName)
}

func (m *matcher) isSchemaIncluded(schemaName string) bool {
	if len(m.includeSchemas) > 0 {
		return matchAny(m.includeSchemas, schemaName)
	}
	return true
}

func (m *matcher) isSchemaExcluded(schemaName string) bool {
	if matchAny(m.excludeSchemas, schemaName) {
		return true
	}
	// System schemas are excluded by default unless explicitly included.
	for _, sys := range m.systemSchemas {
		if schemaName == sys {
			if len(m.includeSchemas) > 0 && matchAny(m.includeSchemas, schemaName) {
				return false
			}
			return true
		}
	}
	return false
}

func compileRegexps(patterns []string) ([]*regexp.Regexp, error) {
	res := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile("^" + p + "$")
		if err != nil {
			return nil, fmt.Errorf("compile regexp '%s': %w", p, err)
		}
		res = append(res, re)
	}
	return res, nil
}

func matchAny(patterns []*regexp.Regexp, s string) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}
