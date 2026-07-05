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

package snapshotbuilder

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// fakeDialect drives the generic builder with canned per-object snapshots.
type fakeDialect struct {
	fn func(core.ObjectDumpSpec) (core.ObjectSnapshot, error)
}

func (f fakeDialect) Object(spec core.ObjectDumpSpec) (core.ObjectSnapshot, error) {
	return f.fn(spec)
}

func (f fakeDialect) Source(source core.SourceSpec) (core.SourceSnapshot, error) {
	return core.SourceSnapshot{Identity: source.Identity}, nil
}

func tableIdentity(database, table string) core.EntityIdentity {
	return core.EntityIdentity{
		Kind:       "test.table",
		NameParts:  []string{"database", "table"},
		NameValues: map[string]string{"database": database, "table": table},
	}
}

func TestBuilder_Build_OverlaysAndSource(t *testing.T) {
	dialect := fakeDialect{fn: func(spec core.ObjectDumpSpec) (core.ObjectSnapshot, error) {
		return core.ObjectSnapshot{SubsetQuery: "q-" + spec.Name}, nil
	}}

	input := core.DumpContext{
		DumpObjectSpecs: []core.ObjectDumpSpec{
			{ObjectID: 1, Name: "users", Identity: tableIdentity("app", "users"), Origin: core.ObjectOrigin{Kind: core.ObjectOriginExplicit}},
			{ObjectID: 2, Name: "logs", Identity: tableIdentity("app", "logs"), Origin: core.ObjectOrigin{Kind: core.ObjectOriginExplicit}},
		},
		Source: core.SourceSpec{
			Engine:   core.DBMSEngineMySQL,
			Identity: core.EntityIdentity{Kind: "test.server", NameParts: []string{"databases"}, NameValues: map[string]string{"databases": "app"}},
		},
	}

	snap, err := New(dialect).Build(context.Background(), input)
	require.NoError(t, err)

	require.Equal(t, core.SnapshotSchemaVersionV1, snap.SchemaVersion)
	require.Equal(t, core.StableKey("test.server:app"), snap.Key)
	require.Equal(t, input.Source.Identity, snap.Source.Identity)
	require.Len(t, snap.Objects, 2)

	users := snap.Objects["test.table:app.users"]
	require.Equal(t, core.StableKey("test.table:app.users"), users.Key)
	require.Equal(t, tableIdentity("app", "users"), users.Identity)
	require.Equal(t, "q-users", users.SubsetQuery)
	require.Equal(t, core.ObjectOrigin{Kind: core.ObjectOriginExplicit}, users.Origin)
}

func TestBuilder_Build_DialectError(t *testing.T) {
	dialect := fakeDialect{fn: func(core.ObjectDumpSpec) (core.ObjectSnapshot, error) {
		return core.ObjectSnapshot{}, fmt.Errorf("boom")
	}}
	input := core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{{ObjectID: 1, Name: "users"}}}

	_, err := New(dialect).Build(context.Background(), input)
	require.ErrorContains(t, err, "boom")
}

func TestBuilder_Build_MissingSourceIdentity(t *testing.T) {
	dialect := fakeDialect{fn: func(core.ObjectDumpSpec) (core.ObjectSnapshot, error) {
		return core.ObjectSnapshot{}, nil
	}}
	// Object identity is valid, but no Source.Identity is set ⇒ source key fails.
	input := core.DumpContext{DumpObjectSpecs: []core.ObjectDumpSpec{{ObjectID: 1, Name: "users", Identity: tableIdentity("app", "users")}}}

	_, err := New(dialect).Build(context.Background(), input)
	require.Error(t, err)
}
