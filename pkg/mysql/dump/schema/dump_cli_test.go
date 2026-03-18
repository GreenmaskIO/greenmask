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

package schema

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
)

func TestDumpCli_Run(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		st := mocks.NewStorageMock()
		st.On(
			"PutObject",
			mock.Anything, "schema_testdb.sql",
			mock.Anything,
		).Return(nil)

		d := New(
			utils.NewDefaultCmdProducer(),
			st,
			[]string{"_TEST=1"},
			[]string{},
			commonmodels.MysqlDumpRelatedSettings{
				AllowedSchemas: []string{"testdb"},
				IncludeTables:  map[string][]string{"testdb": nil},
			},
			false,
			false,
		)
		d.executable = "echo"
		ctx := context.Background()
		_, err := d.DumpSchema(ctx)
		require.NoError(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)

		actual := st.Data.String()
		require.Equal(t, "testdb\n", actual)
	})

	t.Run("put object error", func(t *testing.T) {
		st := mocks.NewStorageMock()
		st.On(
			"PutObject",
			mock.Anything, "schema_testdb.sql",
			mock.Anything,
		).Return(errors.New("put error"))

		d := New(
			utils.NewDefaultCmdProducer(),
			st,
			[]string{"_TEST=1"},
			[]string{},
			commonmodels.MysqlDumpRelatedSettings{
				AllowedSchemas: []string{"testdb"},
				IncludeTables:  map[string][]string{"testdb": nil},
			},
			false,
			false,
		)
		ctx := context.Background()
		_, err := d.DumpSchema(ctx)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})

	t.Run("cmdProducer error", func(t *testing.T) {
		st := mocks.NewStorageMock()
		st.On(
			"PutObject",
			mock.Anything, "schema_testdb.sql",
			mock.Anything,
		).Return(nil)

		d := New(
			utils.NewDefaultCmdProducer(),
			st,
			[]string{"_TEST=1"},
			[]string{},
			commonmodels.MysqlDumpRelatedSettings{
				AllowedSchemas: []string{"testdb"},
				IncludeTables:  map[string][]string{"testdb": nil},
			},
			false,
			false,
		)
		d.executable = "121312 unknown command"
		ctx := context.Background()
		_, err := d.DumpSchema(ctx)
		require.Error(t, err)
		st.AssertNumberOfCalls(t, "PutObject", 1)
	})
}
