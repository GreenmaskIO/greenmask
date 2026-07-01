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
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/snapshotbuilder"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
)

var _ snapshotbuilder.Dialect = snapshotDialect{}

// NewDumpContextSnapshotBuilder builds the MySQL DumpContextSnapshotBuilder. The
// RDBMS-generic flow lives in snapshotbuilder.Builder; only the MySQL-specific
// object identity mapping is provided here via snapshotDialect.
func NewDumpContextSnapshotBuilder() core.DumpContextSnapshotBuilder {
	return snapshotbuilder.New(snapshotDialect{})
}

// snapshotDialect supplies the MySQL identity mapping for the shared snapshot
// builder. Supporting additional data-section object kinds (e.g. sequences) is a
// matter of switching on spec.Kind here.
type snapshotDialect struct{}

func (snapshotDialect) Object(spec core.ObjectDumpSpec) (core.ObjectSnapshot, error) {
	payload, ok := spec.Payload.(transformercontext.TableDumpContext)
	if !ok {
		return core.ObjectSnapshot{}, fmt.Errorf("unexpected payload type %T for object %q", spec.Payload, spec.Name)
	}

	// Engine-agnostic snapshot (attributes, subset query, condition,
	// transformations with resolved parameters) generated on demand.
	snapshot, err := payload.GetSnapshot()
	if err != nil {
		return core.ObjectSnapshot{}, fmt.Errorf("object %q snapshot: %w", spec.Name, err)
	}
	return snapshot, nil
}

// Source maps the MySQL SourceSpec into a SourceSnapshot, surfacing the server
// version and vendor parameters from the MySQL source payload.
func (snapshotDialect) Source(source core.SourceSpec) (core.SourceSnapshot, error) {
	payload, ok := source.Payload.(MySQLSourceDatabasePayload)
	if !ok {
		return core.SourceSnapshot{}, fmt.Errorf("unexpected source payload type %T", source.Payload)
	}
	return core.SourceSnapshot{
		Identity:    source.Identity,
		DBMSVersion: payload.Version.FullString,
		// Vendor parameters are run-specific (gtid/binlog/snapshot id); they are
		// recorded but intentionally not hashed, so they don't cause false drift.
		VendorParameters: payload.VendorParameters,
	}, nil
}
