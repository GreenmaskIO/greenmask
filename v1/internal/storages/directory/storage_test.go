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

package directory

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
)

type DirectorySuite struct {
	suite.Suite
	tmpDir string
	st     *Storage
}

func (suite *DirectorySuite) SetupSuite() {
	var err error
	tempDir := os.Getenv("DIRECTORY_TEST_TEMP_DIR")
	if tempDir == "" {
		tempDir = "/tmp"
	}

	suite.tmpDir, err = os.MkdirTemp(tempDir, "directory_storage_unit_test_")
	suite.Require().NoError(err)

	suite.st, err = New(DirectoryConfig{Path: suite.tmpDir})
	suite.Require().NoError(err)
}

func (suite *DirectorySuite) TestPutObject() {
	buf := bytes.NewBuffer(nil)
	buf.Write([]byte("test"))

	err := suite.st.PutObject(context.Background(), "1/2/3/test.txt", buf)
	suite.Require().NoError(err)
}

func (suite *DirectorySuite) TearDownSuite() {
	if err := os.RemoveAll(suite.tmpDir); err != nil {
		log.Warn().Err(err).Msg("error deleting tmp dir")
	}
}

func TestDirectoryStorage(t *testing.T) {
	suite.Run(t, new(DirectorySuite))
}
