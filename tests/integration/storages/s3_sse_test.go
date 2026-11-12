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
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/greenmaskio/storages"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/internal/domains"
)

// ssePartSize is the smallest part size S3 accepts. Combined with a body a few
// times that size it forces the multipart path without moving much data.
const ssePartSize = 5 * 1024 * 1024

// S3SSESuite asserts that the encryption configured on the storage actually
// lands on the stored object, by reading the object metadata back with a raw client.
type S3SSESuite struct {
	suite.Suite
	raw *awss3.Client
}

func (suite *S3SSESuite) SetupSuite() {
	requireS3Settings(&suite.Suite)

	endpoint := storageS3Endpoint
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	suite.raw = awss3.New(awss3.Options{
		BaseEndpoint: aws.String(endpoint),
		Region:       storageS3Region,
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(storageS3AccessKeyId, storageS3SecretAccessKey, ""),
	})
}

// TearDownSuite removes the sse/ objects so they don't leak into other suites
// sharing the prefix, e.g. the S3StorageSuite walking check.
func (suite *S3SSESuite) TearDownSuite() {
	st := suite.newStorage("", "", false, 0)
	suite.Require().NoError(st.DeleteAll(context.Background(), "sse"))
}

// newStorage builds a storage with the shared connection settings plus the
// encryption options under test, through the same builder the CLI uses.
func (suite *S3SSESuite) newStorage(sse, kmsKey string, bucketKey bool, partSize int64) storages.Storager {
	return newS3Storage(&suite.Suite, func(cfg *domains.S3StorageConfig) {
		cfg.SSE = sse
		cfg.KMSKeyARN = kmsKey
		cfg.BucketKeyEnabled = bucketKey
		if partSize > 0 {
			cfg.MaxPartSize = partSize
		}
	})
}

func (suite *S3SSESuite) head(key string) *awss3.HeadObjectOutput {
	out, err := suite.raw.HeadObject(context.Background(), &awss3.HeadObjectInput{
		Bucket: aws.String(storageS3Bucket),
		Key:    aws.String(path.Join(storageS3Prefix, key)),
	})
	suite.Require().NoError(err)
	return out
}

// putAndRead uploads body and reads it back through greenmask, returning what
// came back so callers can confirm encryption did not corrupt the round trip.
func (suite *S3SSESuite) putAndRead(st storages.Storager, key string, body []byte) []byte {
	suite.Require().NoError(st.PutObject(context.Background(), key, bytes.NewReader(body)))

	obj, err := st.GetObject(context.Background(), key)
	suite.Require().NoError(err)
	defer func() { _ = obj.Close() }()

	got, err := io.ReadAll(obj)
	suite.Require().NoError(err)
	return got
}

func (suite *S3SSESuite) TestNoSSE() {
	st := suite.newStorage("", "", false, 0)
	body := []byte("no encryption configured")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/none.txt", body))
	suite.Assert().Empty(suite.head("sse/none.txt").ServerSideEncryption)
}

func (suite *S3SSESuite) TestSSES3() {
	st := suite.newStorage(string(types.ServerSideEncryptionAes256), "", false, 0)
	body := []byte("encrypted with sse-s3")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/aes256.txt", body))

	out := suite.head("sse/aes256.txt")
	suite.Assert().Equal(types.ServerSideEncryptionAes256, out.ServerSideEncryption, "object stored without encryption")
}

func (suite *S3SSESuite) TestSSEKMS() {
	if storageS3KMSKey == "" {
		suite.T().Skip("-storageS3KMSKey not set, skipping sse-kms")
	}
	st := suite.newStorage(string(types.ServerSideEncryptionAwsKms), storageS3KMSKey, false, 0)
	body := []byte("encrypted with sse-kms")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/kms.txt", body))

	out := suite.head("sse/kms.txt")
	suite.Assert().Equal(types.ServerSideEncryptionAwsKms, out.ServerSideEncryption, "object stored without encryption")
	suite.Require().NotNil(out.SSEKMSKeyId, "object stored without a kms key")
	suite.Assert().Contains(*out.SSEKMSKeyId, storageS3KMSKey)
}

// TestSSEMultipart covers the case the unit tests cannot reach: a dump larger
// than MaxPartSize goes through CreateMultipartUpload, and losing the headers
// there would leave big dumps unencrypted while small ones looked fine.
func (suite *S3SSESuite) TestSSEMultipart() {
	st := suite.newStorage(string(types.ServerSideEncryptionAes256), "", false, ssePartSize)

	body := bytes.Repeat([]byte("greenmask-sse-multipart-payload!"), (ssePartSize*5/32)+1)
	suite.Require().Greater(int64(len(body)), int64(ssePartSize), "payload must exceed one part")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/multipart.bin", body))

	out := suite.head("sse/multipart.bin")
	suite.Assert().Equal(types.ServerSideEncryptionAes256, out.ServerSideEncryption, "multipart upload lost encryption")
	suite.Require().NotNil(out.ContentLength)
	suite.Assert().Equal(int64(len(body)), *out.ContentLength)
}

func (suite *S3SSESuite) TestSSEKMSMultipartWithBucketKey() {
	if storageS3KMSKey == "" {
		suite.T().Skip("-storageS3KMSKey not set, skipping sse-kms")
	}
	st := suite.newStorage(string(types.ServerSideEncryptionAwsKms), storageS3KMSKey, true, ssePartSize)

	body := bytes.Repeat([]byte("greenmask-sse-kms-multipart-payl"), (ssePartSize*5/32)+1)
	suite.Require().Equal(body, suite.putAndRead(st, "sse/kms-multipart.bin", body))

	out := suite.head("sse/kms-multipart.bin")
	suite.Assert().Equal(types.ServerSideEncryptionAwsKms, out.ServerSideEncryption, "multipart upload lost encryption")
	suite.Require().NotNil(out.SSEKMSKeyId, "multipart upload lost the kms key")
}
