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
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

var _ core.IntrospectorV2 = (*IntrospectorV2)(nil)

// IntrospectorV2 introspects the MySQL schema against the session's
// snapshot-synchronized operational DB and folds the result into the pipeline's
// IntrospectionResult.
//
// Scope: introspection applies database/schema-level scoping only — it skips
// excluded schemas/databases (we should not, and may not be permitted to,
// introspect databases the user excluded) and introspects EVERY table in the
// allowed schemas. Table/data include/exclude is deliberately NOT applied here;
// that is the ObjectFilter layer's responsibility. Introspection is therefore
// config-dependent at the schema level (a schema brought in/out of scope between
// runs will show up in schema-drift comparison).
//
// The resulting KindsMap holds tables under kinds.ObjectKindTable (each Object
// carrying the engine-specific *mysqlmodels.Table payload) and one kinds.ObjectKindDatabase
// object per allowed schema, so the schema dump can reference databases by
// runtime ObjectID.
type IntrospectorV2 struct{}

// NewIntrospectorV2 builds the MySQL introspector. The schema/database
// include/exclude scope is supplied per-run via the FilterConfig passed to
// Introspect, so the introspector itself is stateless.
func NewIntrospectorV2() *IntrospectorV2 {
	return &IntrospectorV2{}
}

func (s *IntrospectorV2) Introspect(ctx context.Context, session core.DatabaseSession, filterConfig core.FilterConfig) (core.IntrospectionResult, error) {
	scope, err := newSchemaScope(
		filterConfig.IncludeSchema,
		filterConfig.ExcludeSchema,
		filterConfig.IncludeDatabase,
		filterConfig.ExcludeDatabase,
	)
	if err != nil {
		return core.IntrospectionResult{}, fmt.Errorf("build schema scope: %w", err)
	}
	engine := newIntrospectEngine(scope)

	// The session owns the snapshot-synchronized operational DB; scope
	// introspection to it via RunWithOperationalDB so the session controls the
	// connection lifecycle.
	if err := session.RunWithOperationalDB(ctx, func(ctx context.Context, db core.DB) error {
		return engine.introspect(ctx, db)
	}); err != nil {
		return core.IntrospectionResult{}, fmt.Errorf("introspect mysql: %w", err)
	}

	tableObjects := make([]core.Object, 0, len(engine.tables))
	for idx := range engine.tables {
		t := engine.tables[idx]
		tableObjects = append(tableObjects, core.Object{
			ID:      core.ObjectID(t.ID),
			Kind:    kinds.ObjectKindTable,
			Name:    t.Name,
			Payload: &t,
		})
	}

	// One database object per allowed schema (a schema section, delegated to
	// mysqldump — greenmask does not dump its DDL itself). ObjectID is a per-run
	// handle in the database id space.
	databaseObjects := make([]core.Object, 0, len(engine.allowedSchemas))
	for i, schema := range engine.allowedSchemas {
		databaseObjects = append(databaseObjects, core.Object{
			ID:   core.ObjectID(i),
			Kind: kinds.ObjectKindDatabase,
			Name: schema,
		})
	}

	return core.IntrospectionResult{
		Engine:  core.DBMSEngineMySQL,
		Version: engine.version,
		KindsMap: map[core.ObjectKind][]core.Object{
			kinds.ObjectKindTable:    tableObjects,
			kinds.ObjectKindDatabase: databaseObjects,
		},
	}, nil
}
