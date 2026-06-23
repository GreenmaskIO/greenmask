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

package core

import "context"

// RestoreIntrospectionResult carries the runtime facts read from the target
// database before restore executes.
type RestoreIntrospectionResult struct {
	Version DBMSVersion
	// Vendor is the detected server vendor (e.g. "mysql", "mariadb", "percona"),
	// surfaced as a first-class field for compatibility checks. Mirrors
	// Version.Vendor().
	Vendor string
}

// RestoreIntrospector reads runtime facts about the TARGET database before
// restore executes, querying through the live restore session (opened and
// initialized by the pipeline runtime) rather than a separate connection.
type RestoreIntrospector interface {
	Introspect(ctx context.Context, session DatabaseSession) (RestoreIntrospectionResult, error)
}
