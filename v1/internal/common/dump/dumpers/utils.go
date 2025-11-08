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
	"maps"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func getUniqueDumpTaskID(dumperType string, meta map[string]any) string {
	tableSchema, ok := meta[commonmodels.MetaKeyTableSchema].(string)
	if !ok {
		tableSchema = "!!!UNKNOWN!!!"
	}
	tableName, ok := meta[commonmodels.MetaKeyTableName].(string)
	if !ok {
		tableName = "!!!UNKNOWN!!!"
	}
	meta = maps.Clone(meta)
	return fmt.Sprintf(
		"%s___%s.%s", dumperType, tableSchema, tableName,
	)
}
