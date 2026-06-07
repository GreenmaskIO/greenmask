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

package dump

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/introspect"
)

var _ core.IntrospectorV2 = (*IntrospectorV2)(nil)

// IntrospectorV2 introspects the MySQL schema against the session's
// snapshot-synchronized operational DB and folds the result into the pipeline's
// IntrospectionResult.
//
// It introspects the whole database — no include/exclude filtering is applied
// here. Introspection must stay config-independent so it can be compared across
// runs (schema drift) and persisted into Metadata. The dump scope (which objects
// to actually dump, plus the vendor-CLI include/exclude lists) is a derived
// artifact computed by a later context-building stage from config.Dump and this
// result; see core.IntrospectionResult.
//
// MySQL exposes only one kind of dumpable object — the table — so the resulting
// KindsMap has a single ObjectKindTable entry, with each Object carrying the
// engine-specific *mysqlmodels.Table as its payload.
type IntrospectorV2 struct{}

func (s *IntrospectorV2) Introspect(ctx context.Context, session core.DumpSession) (core.IntrospectionResult, error) {
	// Permissive (empty) options: introspect every user table without applying
	// include/exclude filters. Filtering is the job of the downstream scope stage.
	introspector, err := introspect.NewIntrospector(&config.CommonDumpOptions{})
	if err != nil {
		return core.IntrospectionResult{}, fmt.Errorf("create mysql introspector: %w", err)
	}

	// The session owns the snapshot-synchronized operational DB; scope
	// introspection to it via RunWithOperationalDB so the session controls the
	// connection lifecycle.
	if err := session.RunWithOperationalDB(ctx, func(ctx context.Context, db core.DB) error {
		return introspector.Introspect(ctx, db)
	}); err != nil {
		return core.IntrospectionResult{}, fmt.Errorf("introspect mysql: %w", err)
	}

	tables := introspector.GetTables()
	objects := make([]core.Object, 0, len(tables))
	for idx := range tables {
		t := tables[idx]
		objects = append(objects, core.Object{
			ID:      core.ObjectID(t.ID),
			Kind:    core.ObjectKindTable,
			Name:    t.Name,
			Payload: &t,
		})
	}

	return core.IntrospectionResult{
		Engine: core.DBMSEngineMySQL,
		KindsMap: map[core.ObjectKind][]core.Object{
			core.ObjectKindTable: objects,
		},
	}, nil
}
