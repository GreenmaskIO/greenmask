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
	"flag"
	"os"

	"github.com/rs/zerolog"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

var (
	storageS3Endpoint        string
	storageS3Bucket          string
	storageS3Region          string
	storageS3AccessKeyId     string
	storageS3SecretAccessKey string
	storageS3Prefix          string
)

const (
	storageS3EndpointEnvVarName        = "STORAGE_S3_ENDPOINT"
	storageS3BucketEnvVarName          = "STORAGE_S3_BUCKET"
	storageS3RegionEnvVarName          = "STORAGE_S3_REGION"
	storageS3AccessKeyIdEnvVarName     = "STORAGE_S3_ACCESS_KEY_ID"
	storageS3SecretAccessKeyEnvVarName = "STORAGE_S3_SECRET_KEY"
	storageS3PrefixEnvVarName          = "STORAGE_S3_PREFIX"
)

func init() {
	flag.StringVar(&storageS3Endpoint, "storageS3Endpoint", "", "s3 endpoint")
	flag.StringVar(&storageS3Bucket, "storageS3Bucket", "", "s3 bucket name")
	flag.StringVar(&storageS3Region, "storageS3Region", "", "s3 region name")
	flag.StringVar(&storageS3AccessKeyId, "storageS3AccessKeyId", "", "s3 access key id")
	flag.StringVar(&storageS3SecretAccessKey, "storageS3SecretAccessKey", "", "s3 secred access key")
	flag.StringVar(&storageS3Prefix, "storageS3Prefix", "", "prefix in s3 bucket path")

	if v := os.Getenv(storageS3EndpointEnvVarName); v != "" {
		storageS3Endpoint = v
	}
	if v := os.Getenv(storageS3BucketEnvVarName); v != "" {
		storageS3Bucket = v
	}
	if v := os.Getenv(storageS3RegionEnvVarName); v != "" {
		storageS3Region = v
	}
	if v := os.Getenv(storageS3AccessKeyIdEnvVarName); v != "" {
		storageS3AccessKeyId = v
	}
	if v := os.Getenv(storageS3SecretAccessKeyEnvVarName); v != "" {
		storageS3SecretAccessKey = v
	}
	if v := os.Getenv(storageS3PrefixEnvVarName); v != "" {
		storageS3Prefix = v
	}

}

func init() {
	if err := logger.SetLogLevel(zerolog.LevelDebugValue, logger.LogFormatTextValue); err != nil {
		panic(err)
	}
}
