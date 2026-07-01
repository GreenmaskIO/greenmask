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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// testTable is a stand-in for an engine-specific Object.Payload.
type testTable struct {
	schema string
	name   string
}

func resolveTestTable(obj core.Object) (Identity, bool) {
	t, ok := obj.Payload.(testTable)
	if !ok {
		return Identity{}, false
	}
	return Identity{Schema: t.schema, Name: t.name}, true
}

func tableObject(id core.ObjectID, kind core.ObjectKind, schema, name string) core.Object {
	return core.Object{
		ID:      id,
		Kind:    kind,
		Name:    name,
		Payload: testTable{schema: schema, name: name},
	}
}

func introspection(kinds map[core.ObjectKind][]core.Object) core.IntrospectionResult {
	return core.IntrospectionResult{Engine: core.DBMSEngineMySQL, KindsMap: kinds}
}

func TestFilter_FilterObjects_includeExclude(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
	})

	input := core.ObjectFilterInput{
		FilterConfig: core.FilterConfig{
			IncludeTable: []string{`public\..*`},
			ExcludeTable: []string{`public\.tmp_.*`},
		},
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				tableObject(1, core.ObjectKindTable, "public", "users"),
				tableObject(2, core.ObjectKindTable, "public", "orders"),
				tableObject(3, core.ObjectKindTable, "public", "tmp_cache"),
				tableObject(4, core.ObjectKindTable, "private", "secrets"),
			},
		}),
	}

	res, err := f.FilterObjects(context.Background(), input)
	require.NoError(t, err)
	assert.ElementsMatch(t, []core.ObjectID{1, 2}, res.AllowedObjects[core.ObjectKindTable])
}

func TestFilter_FilterObjects_noFiltersAllowsAll(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
	})

	input := core.ObjectFilterInput{
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				tableObject(1, core.ObjectKindTable, "public", "users"),
				tableObject(2, core.ObjectKindTable, "public", "orders"),
			},
		}),
	}

	res, err := f.FilterObjects(context.Background(), input)
	require.NoError(t, err)
	assert.ElementsMatch(t, []core.ObjectID{1, 2}, res.AllowedObjects[core.ObjectKindTable])
}

func TestFilter_FilterObjects_systemSchemaExcluded(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
		SystemSchemas: mysqlSystemSchemas,
	})

	input := core.ObjectFilterInput{
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				tableObject(1, core.ObjectKindTable, "public", "users"),
				tableObject(2, core.ObjectKindTable, "mysql", "user"),
				tableObject(3, core.ObjectKindTable, "information_schema", "tables"),
			},
		}),
	}

	res, err := f.FilterObjects(context.Background(), input)
	require.NoError(t, err)
	assert.ElementsMatch(t, []core.ObjectID{1}, res.AllowedObjects[core.ObjectKindTable])
}

// PostgreSQL filters tables and sequences with the same patterns: both are
// relations and must be selectable by the same include/exclude rules.
func TestFilter_FilterObjects_multipleRelationKinds(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable, core.ObjectKind("sequence")},
		Resolve:       resolveTestTable,
	})

	input := core.ObjectFilterInput{
		FilterConfig: core.FilterConfig{
			IncludeSchema: []string{"public"},
		},
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				tableObject(1, core.ObjectKindTable, "public", "users"),
				tableObject(2, core.ObjectKindTable, "private", "secrets"),
			},
			core.ObjectKind("sequence"): {
				tableObject(10, core.ObjectKind("sequence"), "public", "users_id_seq"),
				tableObject(11, core.ObjectKind("sequence"), "private", "secrets_id_seq"),
			},
		}),
	}

	res, err := f.FilterObjects(context.Background(), input)
	require.NoError(t, err)
	assert.ElementsMatch(t, []core.ObjectID{1}, res.AllowedObjects[core.ObjectKindTable])
	assert.ElementsMatch(t, []core.ObjectID{10}, res.AllowedObjects[core.ObjectKind("sequence")])
}

func TestFilter_FilterObjects_unresolvedIdentityKept(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
	})

	input := core.ObjectFilterInput{
		FilterConfig: core.FilterConfig{
			IncludeTable: []string{`public\.users`},
		},
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {
				tableObject(1, core.ObjectKindTable, "public", "users"),
				// No resolvable identity (nil payload) -> always kept.
				{ID: 2, Kind: core.ObjectKindTable, Name: "mystery", Payload: nil},
			},
		}),
	}

	res, err := f.FilterObjects(context.Background(), input)
	require.NoError(t, err)
	assert.ElementsMatch(t, []core.ObjectID{1, 2}, res.AllowedObjects[core.ObjectKindTable])
}

func TestFilter_FilterObjects_kindNotPresent(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
	})

	res, err := f.FilterObjects(context.Background(), core.ObjectFilterInput{
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{}),
	})
	require.NoError(t, err)
	assert.Empty(t, res.AllowedObjects[core.ObjectKindTable])
}

func TestFilter_FilterObjects_nilResolver(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
	})

	_, err := f.FilterObjects(context.Background(), core.ObjectFilterInput{
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {tableObject(1, core.ObjectKindTable, "public", "users")},
		}),
	})
	require.Error(t, err)
}

func TestFilter_FilterObjects_invalidRegexp(t *testing.T) {
	f := New(Options{
		RelationKinds: []core.ObjectKind{core.ObjectKindTable},
		Resolve:       resolveTestTable,
	})

	_, err := f.FilterObjects(context.Background(), core.ObjectFilterInput{
		FilterConfig: core.FilterConfig{IncludeTable: []string{"("}},
		IntrospectionResult: introspection(map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: {tableObject(1, core.ObjectKindTable, "public", "users")},
		}),
	})
	require.Error(t, err)
}
