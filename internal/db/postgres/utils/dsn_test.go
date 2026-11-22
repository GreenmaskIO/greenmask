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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHideDbNameInArgs(t *testing.T) {
	tests := []struct {
		name   string
		dbName string
		want   string
	}{
		{"URI", "postgresql://alice:s3cret@db:5432/shop", "***"},
		{"short URI scheme", "postgres://alice:s3cret@db/shop", "***"},
		{"keyword/value", `host=db password=s3\ cret dbname=shop`, "***"},
		{"plain database name", "shop", "shop"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := []string{"--format", "d", "--dbname", tt.dbName, "--host", "db"}

			hidden := HideDbNameInArgs(args)

			assert.Equal(t, []string{"--format", "d", "--dbname", tt.want, "--host", "db"}, hidden)
			assert.Equal(t, tt.dbName, args[3], "the arguments passed to the command must not change")
		})
	}
}
