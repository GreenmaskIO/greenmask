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
	"errors"
	"fmt"
	"regexp"
	"strings"
)

var (
	ErrInternalSchema = errors.New("internal schema requested")
)

func repeatPlaceholder(count int) string {
	if count <= 0 {
		return ""
	}
	return strings.Repeat("?,", count-1) + "?"
}

type objectMatcher struct {
	includeTables           []*regexp.Regexp
	excludeTables           []*regexp.Regexp
	includeTableDefinitions []*regexp.Regexp
	excludeTableDefinitions []*regexp.Regexp
	excludeTableData        []*regexp.Regexp
	includeTableData        []*regexp.Regexp
	includeSchemas          []*regexp.Regexp
	excludeSchemas          []*regexp.Regexp
}

func newObjectMatcher(opt options) (*objectMatcher, error) {
	isList := append(opt.GetIncludedSchemas(), opt.GetIncludedDatabases()...)
	for _, s := range isList {
		if s == "information_schema" || s == "performance_schema" || s == "sys" {
			return nil, fmt.Errorf("%w: %q", ErrInternalSchema, s)
		}
	}

	it, err := compileRegexps(opt.GetIncludedTables())
	if err != nil {
		return nil, fmt.Errorf("compile include tables regexps: %w", err)
	}

	et, err := compileRegexps(opt.GetExcludedTables())
	if err != nil {
		return nil, fmt.Errorf("compile exclude tables regexps: %w", err)
	}

	itd, err := compileRegexps(opt.GetIncludedTableDefinitions())
	if err != nil {
		return nil, fmt.Errorf("cannot compile included table definitions regexps: %w", err)
	}
	etd, err := compileRegexps(opt.GetExcludedTableDefinitions())
	if err != nil {
		return nil, fmt.Errorf("cannot compile excluded table definitions regexps: %w", err)
	}

	extd, err := compileRegexps(opt.GetExcludedTableData())
	if err != nil {
		return nil, fmt.Errorf("compile exclude table data regexps: %w", err)
	}

	intd, err := compileRegexps(opt.GetIncludedTableData())
	if err != nil {
		return nil, fmt.Errorf("compile include table data regexps: %w", err)
	}

	is, err := compileRegexps(isList)
	if err != nil {
		return nil, fmt.Errorf("compile include schemas regexps: %w", err)
	}

	esList := append(opt.GetExcludedSchemas(), opt.GetExcludedDatabases()...)
	es, err := compileRegexps(esList)
	if err != nil {
		return nil, fmt.Errorf("compile exclude schemas regexps: %w", err)
	}

	return &objectMatcher{
		includeTables:           it,
		excludeTables:           et,
		includeTableDefinitions: itd,
		excludeTableDefinitions: etd,
		excludeTableData:        extd,
		includeTableData:        intd,
		includeSchemas:          is,
		excludeSchemas:          es,
	}, nil
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

func (tm *objectMatcher) isTableDataExcluded(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !tm.MatchSchemaIsAllowed(schemaName) {
		return false
	}
	return matchAny(tm.excludeTableData, fullTableName) || matchAny(tm.excludeTables, fullTableName)
}

func (tm *objectMatcher) MatchNeedDumpSchema(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !tm.MatchSchemaIsAllowed(schemaName) {
		return false
	}

	// Check exclusions
	if matchAny(tm.excludeTables, fullTableName) || matchAny(tm.excludeTableDefinitions, fullTableName) {
		return false
	}

	// Check inclusions
	if len(tm.includeTables) > 0 || len(tm.includeTableDefinitions) > 0 {
		return matchAny(tm.includeTables, fullTableName) || matchAny(tm.includeTableDefinitions, fullTableName)
	}

	return true
}

func (tm *objectMatcher) MatchNeedDumpData(schemaName, tableName string) bool {
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	if !tm.MatchSchemaIsAllowed(schemaName) {
		return false
	}

	// Check exclusions
	if tm.isTableDataExcluded(schemaName, tableName) {
		return false
	}

	// Check inclusions
	// If explicit data inclusions exist, they take precedence for data dump
	if len(tm.includeTableData) > 0 {
		return matchAny(tm.includeTableData, fullTableName)
	}

	// If table inclusions exist (schema + data), respect them
	if len(tm.includeTables) > 0 {
		return matchAny(tm.includeTables, fullTableName)
	}

	// If table definitions are explicitly included but not the tables themselves,
	// it usually means schema-only for those objects.
	if len(tm.includeTableDefinitions) > 0 {
		return false
	}

	return true
}

func (tm *objectMatcher) MatchSchemaIsAllowed(schemaName string) bool {
	if !tm.isSchemaIncluded(schemaName) {
		return false
	}

	if tm.isSchemaExcluded(schemaName) {
		return false
	}

	return true
}

func (tm *objectMatcher) isSchemaIncluded(schemaName string) bool {
	if len(tm.includeSchemas) > 0 {
		return matchAny(tm.includeSchemas, schemaName)
	}
	return true
}

func (tm *objectMatcher) isSchemaExcluded(schemaName string) bool {
	if matchAny(tm.excludeSchemas, schemaName) {
		return true
	}
	// Default system exclusions
	if schemaName == "information_schema" || schemaName == "mysql" ||
		schemaName == "performance_schema" || schemaName == "sys" {
		// Only exclude if NOT explicitly included
		if len(tm.includeSchemas) > 0 && matchAny(tm.includeSchemas, schemaName) {
			return false
		}
		return true
	}
	return false
}

func matchAny(patterns []*regexp.Regexp, s string) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}
