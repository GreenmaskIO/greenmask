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

package dumpers

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// tableDebugName builds a human-readable identifier ("table <schema>.<name>")
// for a dumper from its data-stream reader Meta map, used only for log lines and
// error wrapping.
func tableDebugName(meta map[string]any) string {
	tableSchema, ok := meta[core.MetaKeyTableSchema].(string)
	if !ok {
		tableSchema = "!!!UNKNOWN!!!"
	}
	tableName, ok := meta[core.MetaKeyTableName].(string)
	if !ok {
		tableName = "!!!UNKNOWN!!!"
	}
	return fmt.Sprintf("table %s.%s", tableSchema, tableName)
}
