package storages

import (
	"flag"
	"os"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
	"github.com/rs/zerolog"
)

var (
	storageS3Endpoint        string
	storageS3Bucket          string
	storageS3Region          string
	storageS3AccessKeyId     string
	storageS3SecretAccessKey string
)

const (
	storageS3EndpointEnvVarName        = "STORAGE_S3_ENDPOINT"
	storageS3BucketEnvVarName          = "STORAGE_S3_BUCKET"
	storageS3RegionEnvVarName          = "STORAGE_S3_REGION"
	storageS3AccessKeyIdEnvVarName     = "STORAGE_S3_ACCESS_KEY_ID"
	storageS3SecretAccessKeyEnvVarName = "STORAGE_S3_SECRET_KEY"
)

func init() {
	flag.StringVar(&storageS3Endpoint, "storageS3Endpoint", "", "s3 endpoint")
	flag.StringVar(&storageS3Bucket, "storageS3Bucket", "", "s3 bucket name")
	flag.StringVar(&storageS3Region, "storageS3Region", "", "s3 region name")
	flag.StringVar(&storageS3AccessKeyId, "storageS3AccessKeyId", "", "s3 access key id")
	flag.StringVar(&storageS3SecretAccessKey, "storageS3SecretAccessKey", "", "s3 secred access key")

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

}

func init() {
	if err := logger.SetLogLevel(zerolog.LevelDebugValue, logger.LogFormatTextValue); err != nil {
		panic(err)
	}
}
