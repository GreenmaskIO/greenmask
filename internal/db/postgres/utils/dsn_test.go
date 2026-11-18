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

func TestHideCredentials(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{
			name: "uri with user and password",
			dsn:  "postgresql://alice:s3cret@db.example.com:5432/shop",
			want: "postgresql://***:***@db.example.com:5432/shop",
		},
		{
			name: "postgres scheme",
			dsn:  "postgres://alice:s3cret@localhost/shop?sslmode=disable",
			want: "postgres://***:***@localhost/shop?sslmode=disable",
		},
		{
			name: "uri with user only",
			dsn:  "postgresql://alice@localhost/shop",
			want: "postgresql://***@localhost/shop",
		},
		{
			name: "uri with an escaped password",
			dsn:  "postgresql://alice:p%40ss%3Aword@localhost/shop",
			want: "postgresql://***:***@localhost/shop",
		},
		{
			name: "uri with credentials in the query",
			dsn:  "postgresql://localhost/shop?user=alice&sslmode=require&password=s3cret",
			want: "postgresql://localhost/shop?user=***&sslmode=require&password=***",
		},
		{
			name: "uri without credentials",
			dsn:  "postgresql://localhost:5432/shop?sslmode=disable",
			want: "postgresql://localhost:5432/shop?sslmode=disable",
		},
		{
			name: "uri with a socket directory",
			dsn:  "postgresql://alice:s3cret@/shop?host=/var/run/postgresql",
			want: "postgresql://***:***@/shop?host=/var/run/postgresql",
		},
		{
			name: "unparsable uri is hidden whole",
			dsn:  "postgresql://alice:s3cret@local host:bad/shop",
			want: "***",
		},
		{
			name: "keyword/value",
			dsn:  "host=localhost port=5432 user=alice password=s3cret dbname=shop",
			want: "host=localhost port=5432 user=*** password=*** dbname=shop",
		},
		{
			name: "keyword/value with a quoted password",
			dsn:  "host=localhost user=alice password='s3 cr\\'et' dbname=shop",
			want: "host=localhost user=*** password=*** dbname=shop",
		},
		{
			name: "keyword/value with spaces around =",
			dsn:  "host=localhost user = alice password = s3cret dbname=shop",
			want: "host=localhost user = *** password = *** dbname=shop",
		},
		{
			name: "keyword/value in upper case",
			dsn:  "HOST=localhost USER=alice PASSWORD=s3cret",
			want: "HOST=localhost USER=*** PASSWORD=***",
		},
		{
			name: "keyword/value without credentials",
			dsn:  "host=localhost dbname=shop",
			want: "host=localhost dbname=shop",
		},
		{
			name: "dbname and sslpassword are not user or password",
			dsn:  "dbname=user_db sslpassword=keypass",
			want: "dbname=user_db sslpassword=keypass",
		},
		{
			name: "plain database name",
			dsn:  "shop",
			want: "shop",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := HideCredentials(tt.dsn)
			assert.Equal(t, tt.want, got)
			assert.NotContains(t, got, "s3cret")
			assert.NotContains(t, got, "alice")
		})
	}
}

func TestHideCredentialsInArgs(t *testing.T) {
	args := []string{
		"--file", "/tmp/dump", "--dbname", "postgresql://alice:s3cret@localhost/shop", "--jobs", "2",
	}
	original := append([]string(nil), args...)

	got := HideCredentialsInArgs(args)

	assert.Equal(t, []string{
		"--file", "/tmp/dump", "--dbname", "postgresql://***:***@localhost/shop", "--jobs", "2",
	}, got)
	assert.Equal(t, original, args, "the arguments passed to the command must not change")

	assert.Equal(t,
		[]string{"--dbname=host=localhost user=*** password=***"},
		HideCredentialsInArgs([]string{"--dbname=host=localhost user=alice password=s3cret"}),
	)
	assert.Equal(t, []string{"--dbname"}, HideCredentialsInArgs([]string{"--dbname"}))
}
