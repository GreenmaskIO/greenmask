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
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/pkg/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	mysqlconfig "github.com/greenmaskio/greenmask/pkg/mysql/config"
	"github.com/greenmaskio/greenmask/pkg/testutils"
)

type restoreSuite struct {
	testutils.MySQLContainerSuite
}

func (s *restoreSuite) TestMySQLContainerSuite() {
	s.Require().NotNil(s.Container)
}

func TestMySQL(t *testing.T) {
	suite.Run(t, new(restoreSuite))
}

type mockOptions struct {
	mysqlconfig.ConnectionOpts
	VendorOptions []string
}

func (m *mockOptions) SchemaRestoreParams() ([]string, error) {
	params := m.Params()
	params = append(params, m.VendorOptions...)
	return params, nil
}

func (s *restoreSuite) TestRestorer_RestoreSchema() {
	err := utils.SetDefaultContextLogger(zerolog.LevelDebugValue, utils.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	r, err := os.Open(filepath.Join("testdata", "schema.sql"))
	s.Require().NoError(err)
	defer r.Close()
	schemaMeta := &commonmodels.SchemaDumpMetadata{
		DumpedDatabaseSchema: []commonmodels.DumpedDatabaseSchemaStat{
			{
				DatabaseName: "testdb",
				FileName:     "schema_testdb.sql",
				Compression:  commonmodels.CompressionNone,
			},
		},
	}

	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, "schema_testdb.sql").
		Return(r, nil)

	opts := s.GetRootConnectionOpts(ctx)
	rr := NewRestorer(st, &mockOptions{
		ConnectionOpts: opts,
		VendorOptions:  []string{"--verbose"},
	}, utils.NewDefaultCmdProducer(), schemaMeta)
	err = rr.RestoreSchema(ctx)
	s.Require().NoError(err)
}
