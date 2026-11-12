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
	"slices"

	"github.com/greenmaskio/storages"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/builder"
)

type S3StorageSuite struct {
	suite.Suite
	st storages.Storager
}

func requireS3Settings(s *suite.Suite) {
	s.Require().NotEmpty(storageS3Endpoint, "-storageS3Endpoint non-empty flag required")
	s.Require().NotEmpty(storageS3Bucket, "-storageS3Bucket non-empty flag required")
	s.Require().NotEmpty(storageS3Region, "-storageS3Region non-empty flag required")
	s.Require().NotEmpty(storageS3AccessKeyId, "-storageS3AccessKeyId non-empty flag required")
	s.Require().NotEmpty(storageS3SecretAccessKey, "-storageS3SecretAccessKey non-empty flag required")
}

// newS3Storage builds the storage the way the CLI does, through builder and its config mapping.
func newS3Storage(s *suite.Suite, tweak func(cfg *domains.S3StorageConfig)) storages.Storager {
	cfg := domains.NewS3StorageConfig()
	cfg.Endpoint = storageS3Endpoint
	cfg.Bucket = storageS3Bucket
	cfg.Region = storageS3Region
	cfg.AccessKeyId = storageS3AccessKeyId
	cfg.SecretAccessKey = storageS3SecretAccessKey
	cfg.Prefix = storageS3Prefix
	if tweak != nil {
		tweak(cfg)
	}
	st, err := builder.GetStorage(
		context.Background(),
		&domains.StorageConfig{Type: builder.S3StorageType, S3: cfg},
		&domains.LogConfig{Level: zerolog.LevelDebugValue},
	)
	s.Require().NoError(err)
	return st
}

// wipe empties the storage; DeleteAll refuses the storage root itself.
func wipe(ctx context.Context, st storages.Storager) error {
	files, dirs, err := st.ListDir(ctx)
	if err != nil {
		return err
	}
	if len(files) > 0 {
		if err := st.Delete(ctx, files...); err != nil {
			return err
		}
	}
	for _, d := range dirs {
		if err := st.DeleteAll(ctx, d.Dirname()); err != nil {
			return err
		}
	}
	return nil
}

func put(s *suite.Suite, st storages.Storager, key, body string) {
	s.Require().NoError(st.PutObject(context.Background(), key, bytes.NewBufferString(body)))
}

func (suite *S3StorageSuite) SetupSuite() {
	requireS3Settings(&suite.Suite)
	suite.st = newS3Storage(&suite.Suite, nil)
	// The bucket outlives runs, so objects left by an interrupted run break the ListDir checks
	suite.Require().NoError(wipe(context.Background(), suite.st))
}

func (suite *S3StorageSuite) TearDownSuite() {
	suite.Require().NoError(wipe(context.Background(), suite.st))
}

func (suite *S3StorageSuite) TestS3Ops() {
	ctx := context.Background()

	suite.Run("put and get object", func() {
		put(&suite.Suite, suite.st, "test.txt", "1234567890")
		put(&suite.Suite, suite.st, "testdb/test.txt", "1234567890")

		obj, err := suite.st.GetObject(ctx, "test.txt")
		suite.Require().NoError(err)
		data, err := io.ReadAll(obj)
		suite.Require().NoError(err)
		suite.Require().NoError(obj.Close())
		suite.Require().Equal("1234567890", string(data))
	})

	suite.Run("missing object", func() {
		_, err := suite.st.GetObject(ctx, "missing.txt")
		suite.Require().ErrorIs(err, storages.ErrFileNotFound)

		exists, err := suite.st.Exists(ctx, "missing.txt")
		suite.Require().NoError(err)
		suite.Require().False(exists)

		stat, err := suite.st.Stat("missing.txt")
		suite.Require().NoError(err)
		suite.Require().False(stat.Exist)
	})

	suite.Run("walking", func() {
		files, dirs, err := suite.st.ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().Equal([]string{"test.txt"}, files)
		suite.Require().Len(dirs, 1)
		suite.Require().Equal("testdb", dirs[0].Dirname())
		// The dump id is the directory name, so the key layout must not change between versions.
		suite.Require().Equal(path.Join(storageS3Prefix, "testdb")+"/", dirs[0].GetCwd())

		files, dirs, err = dirs[0].ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().Equal([]string{"test.txt"}, files)
		suite.Require().Empty(dirs)
	})

	suite.Run("sub storage", func() {
		sub, err := suite.st.SubStorage("testdb", true)
		suite.Require().NoError(err)
		suite.Require().Equal("testdb", sub.Dirname())
		exists, err := sub.Exists(ctx, "test.txt")
		suite.Require().NoError(err)
		suite.Require().True(exists)

		_, err = suite.st.SubStorage("../outside", true)
		suite.Require().ErrorIs(err, storages.ErrUnsafeKey)
	})

	suite.Run("delete", func() {
		put(&suite.Suite, suite.st, "test_to_del.txt", "1234567890")

		files, _, err := suite.st.ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().Contains(files, "test_to_del.txt")

		suite.Require().NoError(suite.st.Delete(ctx, "test_to_del.txt"))

		files, _, err = suite.st.ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().NotContains(files, "test_to_del.txt")
	})

	// DeleteAll sends DeleteObjects, the call where S3-compatible stores may reject v2 checksums.
	suite.Run("delete_all", func() {
		put(&suite.Suite, suite.st, "dir1/test_to_del2.txt", "1234567890")
		put(&suite.Suite, suite.st, "dir1/subdir2/test_to_del3.txt", "1234567890")

		_, dirs, err := suite.st.ListDir(ctx)
		suite.Require().NoError(err)
		idx := slices.IndexFunc(dirs, func(s storages.Storager) bool { return s.Dirname() == "dir1" })
		suite.Require().NotEqual(-1, idx)

		files, dirs, err := dirs[idx].ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().Equal([]string{"test_to_del2.txt"}, files)
		suite.Require().Len(dirs, 1)
		suite.Require().Equal("subdir2", dirs[0].Dirname())

		suite.Require().NoError(suite.st.DeleteAll(ctx, "dir1"))

		_, dirs, err = suite.st.ListDir(ctx)
		suite.Require().NoError(err)
		suite.Require().False(slices.ContainsFunc(dirs, func(s storages.Storager) bool { return s.Dirname() == "dir1" }))
	})

	suite.Run("delete_all refuses the root", func() {
		suite.Require().ErrorIs(suite.st.DeleteAll(ctx, ""), storages.ErrUnsafeKey)
	})
}
