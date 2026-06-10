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

// Package snapshotbuilder provides the RDBMS-generic DumpContextSnapshotBuilder.
//
// The flow — iterate the dump object specs, obtain each object's engine-agnostic
// ObjectSnapshot, overlay identity/key/need-schema-dump, and assemble the source
// snapshot — is shared by every relational engine. Only the identity mapping and
// source grouping differ, and supporting additional data-section object kinds
// (tables, sequences, large objects, ...) is just more cases inside the Dialect.
// Engines provide a Dialect; this package owns everything else.
package snapshotbuilder

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// Dialect supplies the engine-specific pieces of snapshot building. Owning the
// payload type assertion here keeps the generic Builder free of any engine type.
type Dialect interface {
	// Object extracts the engine-agnostic ObjectSnapshot for one spec (attributes,
	// subset query, condition, transformations). The Builder overlays the
	// identity-derived Key/Identity/Source. The object identity itself is taken
	// from the spec (ObjectDumpSpec.Identity), populated by the context builders.
	Object(spec core.ObjectDumpSpec) (core.ObjectSnapshot, error)
	// Source maps the engine-specific SourceSpec (identity + payload: version,
	// vendor parameters, ...) into the SourceSnapshot. The Builder derives the
	// snapshot key from the returned identity.
	Source(source core.SourceSpec) (core.SourceSnapshot, error)
}

var _ core.DumpContextSnapshotBuilder = (*Builder)(nil)

// Builder is the shared DumpContextSnapshotBuilder, parameterized by a Dialect.
type Builder struct {
	dialect Dialect
}

// New builds a Builder backed by the given engine Dialect.
func New(dialect Dialect) *Builder {
	return &Builder{dialect: dialect}
}

func (b *Builder) Build(_ context.Context, input core.DumpContext) (core.DumpContextSnapshot, error) {
	objects := make(map[core.StableKey]core.ObjectSnapshot, len(input.DumpObjectSpecs))
	for i := range input.DumpObjectSpecs {
		spec := input.DumpObjectSpecs[i]

		snapshot, err := b.dialect.Object(spec)
		if err != nil {
			return core.DumpContextSnapshot{}, fmt.Errorf("build object snapshot: %w", err)
		}

		// Identity is carried on the spec by the context builders.
		key, err := spec.Identity.StableKey()
		if err != nil {
			return core.DumpContextSnapshot{}, fmt.Errorf("build object key: %w", err)
		}
		if _, exists := objects[key]; exists {
			return core.DumpContextSnapshot{}, fmt.Errorf("duplicate object snapshot key %q", key)
		}

		snapshot.Key = key
		snapshot.Identity = spec.Identity
		// Whether the object is explicit or derived is decided by the context
		// builders and carried on the spec.
		snapshot.Origin = spec.Origin

		objects[key] = snapshot
	}

	// The source snapshot (identity + version + vendor parameters) is provided by
	// the engine from the SourceSpec, so it is not gathered from the per-object
	// specs here.
	source, err := b.dialect.Source(input.Source)
	if err != nil {
		return core.DumpContextSnapshot{}, fmt.Errorf("build source snapshot: %w", err)
	}
	key, err := source.Identity.StableKey()
	if err != nil {
		return core.DumpContextSnapshot{}, fmt.Errorf("build snapshot key: %w", err)
	}

	return core.DumpContextSnapshot{
		SchemaVersion: core.SnapshotSchemaVersionV1,
		Key:           key,
		Source:        source,
		Objects:       objects,
	}, nil
}
