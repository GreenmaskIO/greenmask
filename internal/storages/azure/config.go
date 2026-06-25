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

package azure

import (
	"fmt"
	"strings"
)

// Defaults and constants ported from wal-g pkg/storages/azure/configure.go
const (
	minBufferSize     = 1024
	defaultBufferSize = 8 * 1024 * 1024
	minBuffers        = 1
	defaultBuffers    = 4
	defaultTryTimeout = 5
	defaultEnvName    = "AzurePublicCloud"
)

// authType identifies which credential method was resolved from the config.
type authType string

const (
	authTypeNotSpecified authType = ""
	authTypeAccessKey    authType = "AzureAccessKeyAuth"
	authTypeSASToken     authType = "AzureSASTokenAuth"
)

type Config struct {
	Container           string `mapstructure:"container"`              // required
	StorageAccount      string `mapstructure:"storage_account"`        // required
	Prefix              string `mapstructure:"prefix"`                 // path within container
	AccessKey           string `mapstructure:"access_key"`             // auth: shared key
	SASToken            string `mapstructure:"sas_token"`              // auth: SAS
	Endpoint            string `mapstructure:"endpoint"`               // full service URL override (path-style, e.g. Azurite/private deployments)
	EndpointSuffix      string `mapstructure:"endpoint_suffix"`        // overrides env-derived suffix
	EnvironmentName     string `mapstructure:"environment_name"`       // AzurePublicCloud (default), AzureUSGovernmentCloud, AzureChinaCloud, AzureGermanCloud
	BufferSize          int    `mapstructure:"buffer_size"`            // upload block size, default 8MiB, min 1KiB
	MaxBuffers          int    `mapstructure:"max_buffers"`            // upload concurrency, default 4, min 1
	TryTimeout          int    `mapstructure:"try_timeout"`            // minutes, default 5
	BlobStoreAPIVersion string `mapstructure:"blob_store_api_version"` // optional x-ms-version override
}

func NewConfig() *Config {
	return &Config{
		EnvironmentName: defaultEnvName,
		BufferSize:      defaultBufferSize,
		MaxBuffers:      defaultBuffers,
		TryTimeout:      defaultTryTimeout,
	}
}

// Validate ensures the required fields are present and clamps the upload
// tuning knobs to their minimums.
func (c *Config) Validate() error {
	if c.Container == "" {
		return fmt.Errorf("container is required")
	}
	if c.StorageAccount == "" {
		return fmt.Errorf("storage_account is required")
	}
	if c.BufferSize < minBufferSize {
		c.BufferSize = minBufferSize
	}
	if c.MaxBuffers < minBuffers {
		c.MaxBuffers = minBuffers
	}
	if c.TryTimeout <= 0 {
		c.TryTimeout = defaultTryTimeout
	}
	if c.EnvironmentName == "" {
		c.EnvironmentName = defaultEnvName
	}
	return nil
}

// resolveAuth picks the credential method based on which secret is set and
// normalizes the SAS token's leading "?". This mirrors wal-g's
// configureAuthType and is factored out so auth dispatch is unit-testable
// without creating a real client.
func resolveAuth(c *Config) (at authType, sasToken, accessKey string) {
	if c.AccessKey != "" {
		return authTypeAccessKey, "", c.AccessKey
	}
	if c.SASToken != "" {
		token := c.SASToken
		// Tokens may or may not begin with "?", normalize these cases.
		if !strings.HasPrefix(token, "?") {
			token = "?" + token
		}
		return authTypeSASToken, token, ""
	}
	return authTypeNotSpecified, "", ""
}

// getStorageEndpointSuffix maps an Azure environment name to its storage
// account endpoint suffix. Expected names are AzureUSGovernmentCloud,
// AzureChinaCloud and AzureGermanCloud; any other name (including the default
// AzurePublicCloud) returns the public-cloud suffix.
func getStorageEndpointSuffix(environmentName string) string {
	switch environmentName {
	case "AzureUSGovernmentCloud":
		return "core.usgovcloudapi.net"
	case "AzureChinaCloud":
		return "core.chinacloudapi.cn"
	case "AzureGermanCloud":
		return "core.cloudapi.de"
	default:
		return "core.windows.net"
	}
}
