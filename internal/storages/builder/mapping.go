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

package builder

import (
	"regexp"

	"github.com/greenmaskio/storages/azure"
	"github.com/greenmaskio/storages/directory"
	"github.com/greenmaskio/storages/s3"
	"github.com/greenmaskio/storages/ssh"

	"github.com/greenmaskio/greenmask/internal/domains"
)

// toS3 keeps the aws-sdk-go v1 meaning of the greenmask keys on top of the v2 SDK.
func toS3(c *domains.S3StorageConfig) s3.Config {
	return s3.Config{
		Endpoint:         normalizeEndpoint(c.Endpoint),
		Bucket:           c.Bucket,
		Prefix:           c.Prefix,
		Region:           c.Region,
		StorageClass:     c.StorageClass,
		DisableSSL:       c.DisableSSL,
		AccessKeyId:      c.AccessKeyId,
		SecretAccessKey:  c.SecretAccessKey,
		SessionToken:     c.SessionToken,
		RoleArn:          c.RoleArn,
		SessionName:      c.SessionName,
		MaxRetries:       maxAttempts(c.MaxRetries),
		CertFile:         c.CertFile,
		MaxPartSize:      c.MaxPartSize,
		Concurrency:      c.Concurrency,
		UseListObjectsV1: c.UseListObjectsV1,
		ForcePathStyle:   c.ForcePathStyle,
		UseAccelerate:    c.UseAccelerate,
		NoVerifySsl:      c.NoVerifySsl,
		SSE:              c.SSE,
		KMSKeyARN:        c.KMSKeyARN,
		BucketKeyEnabled: c.BucketKeyEnabled,
	}
}

// schemeRE is aws-sdk-go v1's test for an endpoint that already has a scheme.
var schemeRE = regexp.MustCompile("^([^:]+)://")

// v2 rejects an endpoint without a scheme, so add https:// as v1's endpoints.AddScheme did.
// v1 used http only with DisableSSL, which greenmask never passed to the SDK.
func normalizeEndpoint(endpoint string) string {
	if endpoint == "" || schemeRE.MatchString(endpoint) {
		return endpoint
	}
	return "https://" + endpoint
}

// v1's max_retries counted retries after the first try, v2's MaxAttempts counts every try.
// Negative meant no retries in v1, and 0 would make v2 use its default, hence the floor of 1.
func maxAttempts(retries int) int {
	return max(retries, 0) + 1
}

func toAzure(c *domains.AzureStorageConfig) azure.Config {
	return azure.Config{
		Container:           c.Container,
		StorageAccount:      c.StorageAccount,
		Prefix:              c.Prefix,
		AccessKey:           c.AccessKey,
		SASToken:            c.SASToken,
		Endpoint:            c.Endpoint,
		EndpointSuffix:      c.EndpointSuffix,
		EnvironmentName:     c.EnvironmentName,
		BufferSize:          c.BufferSize,
		MaxBuffers:          c.MaxBuffers,
		TryTimeout:          c.TryTimeout,
		MaxRetries:          azureRetries(c.MaxRetries),
		BlobStoreAPIVersion: c.BlobStoreAPIVersion,
	}
}

// azureRetries maps max_retries 0 to -1: azcore's RetryOptions reads 0 as its default of 3
// and only a negative value as no retries.
func azureRetries(retries int) int {
	if retries <= 0 {
		return -1
	}
	return retries
}

func toSSH(c *domains.SSHStorageConfig) ssh.Config {
	return ssh.Config{
		Host:                 c.Host,
		Port:                 c.Port,
		User:                 c.User,
		Password:             c.Password,
		PrivateKeyPath:       c.PrivateKeyPath,
		PrivateKeyPassphrase: c.PrivateKeyPassphrase,
		Prefix:               c.Prefix,
		ConnectTimeout:       c.ConnectTimeout,
	}
}

// toDirectory maps path to RootPath and leaves the library's Prefix empty, so dumps stay at
// <path>/<dump_id> as before.
func toDirectory(c *domains.DirectoryStorageConfig) directory.Config {
	return directory.Config{RootPath: c.Path}
}
