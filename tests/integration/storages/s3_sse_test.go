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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/suite"

	gms3 "github.com/greenmaskio/greenmask/internal/storages/s3"
)

// ssePartSize is the smallest part size S3 accepts. Combined with a body a few
// times that size it forces the multipart path without moving much data.
const ssePartSize = 5 * 1024 * 1024

// S3SSESuite asserts that the encryption configured on the storage actually
// lands on the stored object, by reading the object metadata back with a raw
// client. The unit tests can only check the request greenmask builds.
type S3SSESuite struct {
	suite.Suite
	raw *awss3.S3
}

func (suite *S3SSESuite) SetupSuite() {
	suite.Require().NotEmpty(storageS3Endpoint, "-storageS3Endpoint non-empty flag required")
	suite.Require().NotEmpty(storageS3Bucket, "-storageS3Bucket non-empty flag required")
	suite.Require().NotEmpty(storageS3Region, "-storageS3Region non-empty flag required")
	suite.Require().NotEmpty(storageS3AccessKeyId, "-storageS3AccessKeyId non-empty flag required")
	suite.Require().NotEmpty(storageS3SecretAccessKey, "-storageS3SecretAccessKey non-empty flag required")

	ses, err := session.NewSession(
		aws.NewConfig().
			WithEndpoint(storageS3Endpoint).
			WithRegion(storageS3Region).
			WithS3ForcePathStyle(true).
			WithCredentials(credentials.NewStaticCredentials(
				storageS3AccessKeyId, storageS3SecretAccessKey, "",
			)),
	)
	suite.Require().NoError(err)
	suite.raw = awss3.New(ses)
}

// newStorage builds a storage with the shared connection settings plus the
// encryption options under test, running the same validation the CLI runs.
func (suite *S3SSESuite) newStorage(sse, kmsKey string, bucketKey bool, partSize int64) *gms3.Storage {
	cfg := gms3.NewConfig()
	cfg.Endpoint = storageS3Endpoint
	cfg.Bucket = storageS3Bucket
	cfg.Region = storageS3Region
	cfg.AccessKeyId = storageS3AccessKeyId
	cfg.SecretAccessKey = storageS3SecretAccessKey
	cfg.Prefix = storageS3Prefix
	cfg.SSE = sse
	cfg.KMSKeyARN = kmsKey
	cfg.BucketKeyEnabled = bucketKey
	if partSize > 0 {
		cfg.MaxPartSize = partSize
	}

	suite.Require().NoError(cfg.Validate())

	st, err := gms3.NewStorage(context.Background(), cfg, zerolog.LevelDebugValue)
	suite.Require().NoError(err)
	return st
}

func (suite *S3SSESuite) head(key string) *awss3.HeadObjectOutput {
	out, err := suite.raw.HeadObject(&awss3.HeadObjectInput{
		Bucket: aws.String(storageS3Bucket),
		Key:    aws.String(path.Join(storageS3Prefix, key)),
	})
	suite.Require().NoError(err)
	return out
}

// putAndRead uploads body and reads it back through greenmask, returning what
// came back so callers can confirm encryption did not corrupt the round trip.
func (suite *S3SSESuite) putAndRead(st *gms3.Storage, key string, body []byte) []byte {
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
	suite.Assert().Nil(suite.head("sse/none.txt").ServerSideEncryption)
}

func (suite *S3SSESuite) TestSSES3() {
	st := suite.newStorage(awss3.ServerSideEncryptionAes256, "", false, 0)
	body := []byte("encrypted with sse-s3")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/aes256.txt", body))

	out := suite.head("sse/aes256.txt")
	suite.Require().NotNil(out.ServerSideEncryption, "object stored without encryption")
	suite.Assert().Equal(awss3.ServerSideEncryptionAes256, *out.ServerSideEncryption)
}

func (suite *S3SSESuite) TestSSEKMS() {
	if storageS3KMSKey == "" {
		suite.T().Skip("-storageS3KMSKey not set, skipping sse-kms")
	}
	st := suite.newStorage(awss3.ServerSideEncryptionAwsKms, storageS3KMSKey, false, 0)
	body := []byte("encrypted with sse-kms")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/kms.txt", body))

	out := suite.head("sse/kms.txt")
	suite.Require().NotNil(out.ServerSideEncryption, "object stored without encryption")
	suite.Assert().Equal(awss3.ServerSideEncryptionAwsKms, *out.ServerSideEncryption)
	suite.Require().NotNil(out.SSEKMSKeyId, "object stored without a kms key")
	suite.Assert().Contains(*out.SSEKMSKeyId, storageS3KMSKey)
}

// TestSSEMultipart covers the case the unit tests cannot reach: a dump larger
// than MaxPartSize goes through CreateMultipartUpload, and losing the headers
// there would leave big dumps unencrypted while small ones looked fine.
func (suite *S3SSESuite) TestSSEMultipart() {
	st := suite.newStorage(awss3.ServerSideEncryptionAes256, "", false, ssePartSize)

	body := bytes.Repeat([]byte("greenmask-sse-multipart-payload!"), (ssePartSize*5/32)+1)
	suite.Require().Greater(int64(len(body)), int64(ssePartSize), "payload must exceed one part")

	suite.Require().Equal(body, suite.putAndRead(st, "sse/multipart.bin", body))

	out := suite.head("sse/multipart.bin")
	suite.Require().NotNil(out.ServerSideEncryption, "multipart upload lost encryption")
	suite.Assert().Equal(awss3.ServerSideEncryptionAes256, *out.ServerSideEncryption)
	suite.Assert().Equal(int64(len(body)), *out.ContentLength)
}

func (suite *S3SSESuite) TestSSEKMSMultipartWithBucketKey() {
	if storageS3KMSKey == "" {
		suite.T().Skip("-storageS3KMSKey not set, skipping sse-kms")
	}
	st := suite.newStorage(awss3.ServerSideEncryptionAwsKms, storageS3KMSKey, true, ssePartSize)

	body := bytes.Repeat([]byte("greenmask-sse-kms-multipart-payl"), (ssePartSize*5/32)+1)
	suite.Require().Equal(body, suite.putAndRead(st, "sse/kms-multipart.bin", body))

	out := suite.head("sse/kms-multipart.bin")
	suite.Require().NotNil(out.ServerSideEncryption, "multipart upload lost encryption")
	suite.Assert().Equal(awss3.ServerSideEncryptionAwsKms, *out.ServerSideEncryption)
	suite.Require().NotNil(out.SSEKMSKeyId, "multipart upload lost the kms key")
}
