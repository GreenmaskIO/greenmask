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

package restore

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	mysqlversion "github.com/greenmaskio/greenmask/pkg/mysql/version"
)

var _ core.RestoreIntrospector = (*RestoreIntrospector)(nil)

// RestoreIntrospector reads runtime facts about the TARGET MySQL database before
// restore executes. It queries the server version through the live restore
// session (opened and initialized by the pipeline runtime) rather than opening a
// separate connection. It mirrors the dump-side introspectEngine.getVersion.
type RestoreIntrospector struct{}

func (r *RestoreIntrospector) Introspect(
	ctx context.Context,
	session core.DatabaseSession,
) (core.RestoreIntrospectionResult, error) {
	var versionString, versionComment string
	err := core.ExecOnSession(ctx, session, func(ctx context.Context, db core.DB) error {
		return db.QueryRowContext(ctx, "SELECT VERSION(), @@version_comment").
			Scan(&versionString, &versionComment)
	})
	if err != nil {
		return core.RestoreIntrospectionResult{}, fmt.Errorf("query target server version: %w", err)
	}

	version := mysqlversion.ParseServerVersion(versionString, versionComment)
	return core.RestoreIntrospectionResult{
		Version: version,
		Vendor:  version.Vendor(),
	}, nil
}
