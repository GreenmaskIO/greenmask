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

package pgdump

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions_GetParams_excludeExtension(t *testing.T) {
	options := &Options{
		Jobs:            -1,
		Compression:     -1,
		LockWaitTimeout: -1,
		Port:            pgDefaultPort,
	}

	assert.NotContains(t, options.GetParams(), "--exclude-extension")

	options.ExcludeExtension = []string{"pg_stat_statements", "pgcrypto"}
	assert.Equal(
		t,
		[]string{
			"--exclude-extension", "pg_stat_statements",
			"--exclude-extension", "pgcrypto",
		},
		options.GetParams(),
	)
}
