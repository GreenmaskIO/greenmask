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

package interfaces

import (
	"context"

	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
)

type Introspector interface {
	GetCommonTables() []commonmodels.Table
	Introspect(ctx context.Context, tx DB) error
	GetDumpScope() commonmodels.DumpScope
	GetMatchedDatabases() []string
}

type IntrospectorV2 interface {
	// Introspect - receives the dump session and returns introspection results.
	// The introspector obtains a snapshot-synchronized operational DB from the
	// session (via RunWithOperationalDB) for the duration of introspection; it
	// does not roll the underlying transaction back.
	Introspect(ctx context.Context, session DumpSession) (commonmodels.IntrospectionResult, error)
}
