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

	suite.st, err = NewStorage(&Config{Path: suite.tmpDir}, "")
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
