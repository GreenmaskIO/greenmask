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

package storages

import (
	"bytes"
	"context"
	"io"
	"path"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/internal/storages/s3"
)

type S3StorageSuite struct {
	suite.Suite
	cfg *s3.Config
	st  *s3.Storage
}

func (suite *S3StorageSuite) SetupSuite() {
	suite.Require().NotEmpty(storageS3Endpoint, "-storageS3Endpoint non-empty flag required")
	suite.Require().NotEmpty(storageS3Bucket, "-storageS3Bucket non-empty flag required")
	suite.Require().NotEmpty(storageS3Region, "-storageS3Region non-empty flag required")
	suite.Require().NotEmpty(storageS3AccessKeyId, "-storageS3AccessKeyId non-empty flag required")
	suite.Require().NotEmpty(storageS3SecretAccessKey, "-storageS3SecretAccessKey non-empty flag required")
	suite.cfg = s3.NewConfig()
	suite.cfg.Endpoint = storageS3Endpoint
	suite.cfg.Bucket = storageS3Bucket
	suite.cfg.Region = storageS3Region
	suite.cfg.AccessKeyId = storageS3AccessKeyId
	suite.cfg.SecretAccessKey = storageS3SecretAccessKey
	suite.cfg.Prefix = storageS3Prefix

	var err error
	suite.st, err = s3.NewStorage(context.Background(), suite.cfg, zerolog.LevelDebugValue)
	suite.Require().NoError(err)
}

func (suite *S3StorageSuite) TestS3Ops() {
	suite.Run("new storage", func() {
		_, err := s3.NewStorage(context.Background(), suite.cfg, zerolog.LevelDebugValue)
		suite.Require().NoError(err)
	})

	suite.Run("put object", func() {
		buf := bytes.NewBuffer([]byte("1234567890"))
		err := suite.st.PutObject(context.Background(), "/test.txt", buf)
		suite.Require().NoError(err)
		buf = bytes.NewBuffer([]byte("1234567890"))
		err = suite.st.PutObject(context.Background(), "/testdb/test.txt", buf)
		suite.Require().NoError(err)
	})

	suite.Run("get object", func() {
		obj, err := suite.st.GetObject(context.Background(), "/test.txt")
		suite.Require().NoError(err)
		data, err := io.ReadAll(obj)
		suite.Require().NoError(err)
		bytes.Equal(data, []byte("1234567890"))
	})

	suite.Run("walking", func() {
		buf := bytes.NewBuffer([]byte("1234567890"))
		err := suite.st.PutObject(context.Background(), "/test.txt", buf)
		suite.Require().NoError(err)
		buf = bytes.NewBuffer([]byte("1234567890"))
		err = suite.st.PutObject(context.Background(), "/testdb/test.txt", buf)
		suite.Require().NoError(err)

		files, dirs, err := suite.st.ListDir(context.Background())
		suite.Require().NoError(err)
		suite.Require().Len(files, 1)
		suite.Require().Len(dirs, 1)
		suite.Require().Equal("test.txt", files[0])
		s3Dir := dirs[0].(*s3.Storage)
		suite.Require().Equal(path.Join(suite.cfg.Bucket, suite.cfg.Prefix, "testdb")+"/", s3Dir.GetCwd())

		nextDir := dirs[0]
		files, dirs, err = nextDir.ListDir(context.Background())
		suite.Require().NoError(err)
		suite.Require().Len(files, 1)
		suite.Require().Len(dirs, 0)
		suite.Require().Equal("test.txt", files[0])
	})

	suite.Run("delete", func() {
		buf := bytes.NewBuffer([]byte("1234567890"))
		err := suite.st.PutObject(context.Background(), "/test_to_del.txt", buf)
		suite.Require().NoError(err)

		files, _, err := suite.st.ListDir(context.Background())
		suite.Require().NoError(err)
		suite.Require().Contains(files, "test_to_del.txt")

		err = suite.st.Delete(context.Background(), "/test_to_del.txt")
		suite.Require().NoError(err)

		files, _, err = suite.st.ListDir(context.Background())
		suite.Require().NoError(err)
		suite.Require().NotContains(files, "test_to_del.txt")
	})

}
