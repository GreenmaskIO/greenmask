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
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.ObjectFilter = (*Filter)(nil)

// Identity is the schema-qualified identity of an introspected object, matched
// against the include/exclude patterns of core.FilterConfig.
type Identity struct {
	Schema string
	Name   string
}

// IdentityResolver extracts the schema-qualified Identity from an introspected
// object. It is DBMS-specific because Object.Payload is engine-specific.
//
// The boolean result is false when the object carries no resolvable identity; a
// non-identified object is treated as always allowed, leaving it to other stages
// to decide its fate.
type IdentityResolver func(core.Object) (Identity, bool)

// Options configures the generic Filter for a specific DBMS.
type Options struct {
	// RelationKinds are the object kinds subject to table-level include/exclude
	// filtering. For MySQL this is the single table kind; for PostgreSQL it is
	// both tables and sequences, which are both relations and can be
	// included/excluded by the same patterns.
	RelationKinds []core.ObjectKind

	// Resolve extracts the schema-qualified identity of an object. Required.
	Resolve IdentityResolver

	// SystemSchemas are excluded by default unless explicitly included via the
	// FilterConfig include lists. DBMS-specific (e.g. information_schema, mysql,
	// performance_schema, sys for MySQL; pg_catalog, information_schema for
	// PostgreSQL).
	SystemSchemas []string
}

// Filter is the common, DBMS-agnostic implementation of core.ObjectFilter. It
// applies the include/exclude rules in core.FilterConfig to the objects of the
// configured relation kinds and returns the allowed ObjectID set per kind.
//
// It is reusable across engines: a concrete ObjectFilter supplies the relation
// kinds, the system schemas and the identity resolver, and may wrap or extend
// the result (e.g. PostgreSQL filtering both tables and sequences).
type Filter struct {
	relationKinds []core.ObjectKind
	resolve       IdentityResolver
	systemSchemas []string
}

// New builds a generic ObjectFilter from the supplied options.
func New(opts Options) *Filter {
	return &Filter{
		relationKinds: opts.RelationKinds,
		resolve:       opts.Resolve,
		systemSchemas: opts.SystemSchemas,
	}
}

func (f *Filter) FilterObjects(
	_ context.Context,
	input core.ObjectFilterInput,
) (core.ObjectFilterResult, error) {
	if f.resolve == nil {
		return core.ObjectFilterResult{}, fmt.Errorf("object filter: identity resolver is not configured")
	}

	m, err := newMatcher(input.FilterConfig, f.systemSchemas)
	if err != nil {
		return core.ObjectFilterResult{}, fmt.Errorf("build object matcher: %w", err)
	}

	allowed := make(map[core.ObjectKind][]core.ObjectID)
	for _, kind := range f.relationKinds {
		objects := input.IntrospectionResult.KindsMap[kind]
		for i := range objects {
			id, ok := f.resolve(objects[i])
			if !ok {
				// No resolvable identity — leave the object in.
				allowed[kind] = append(allowed[kind], objects[i].ID)
				continue
			}
			if m.isAllowed(id.Schema, id.Name) {
				allowed[kind] = append(allowed[kind], objects[i].ID)
			}
		}
	}

	return core.ObjectFilterResult{AllowedObjects: allowed}, nil
}
