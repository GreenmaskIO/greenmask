// Copyright 2023 Greenmask
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

package context

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildTableSearchQuery(t *testing.T) {
	var includeTable, excludeTable, excludeTableData, includeForeignData, includeSchema, excludeSchema []string
	includeTable = []string{"bookings.*"}
	excludeTable = []string{"booki*.boarding_pas*", "b?*.seats"}
	includeSchema = []string{"booki*"}
	excludeSchema = []string{"public*[[:digit:]]*1"}
	excludeTableData = []string{"bookings.flights"}
	includeForeignData = []string{"myserver"}
	res, err := BuildTableSearchQuery(includeTable, excludeTable, excludeTableData,
		includeForeignData, includeSchema, excludeSchema)
	assert.NoError(t, err)
	fmt.Println(res)
}
