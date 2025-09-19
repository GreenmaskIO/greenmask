package schema

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/mysql/restore/config"
	"github.com/greenmaskio/greenmask/v1/internal/testutils"
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

func (s *restoreSuite) TestRestorer_RestoreSchema() {
	err := utils.SetDefaultContextLogger(zerolog.LevelDebugValue, utils.LogFormatText)
	s.Require().NoError(err)
	ctx := context.Background()

	r, err := os.Open(filepath.Join("testdata", "schema.sql"))
	s.Require().NoError(err)
	defer r.Close()
	st := mocks.NewStorageMock()
	st.On("GetObject", mock.Anything, schemaFileName).
		Return(r, nil)

	opts := s.GetRootConnectionOpts(ctx)
	rr := NewRestorer(st, &config.RestoreOptions{
		ConnectionOpts: opts,
		Verbose:        true,
	})
	err = rr.RestoreSchema(ctx)
	s.Require().NoError(err)
}
