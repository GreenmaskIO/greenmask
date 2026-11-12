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
//go:build viper_bind_struct

package builder

import (
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Run with -tags=viper_bind_struct, as make build does: env binding of nested keys depends on it.
func TestEnvBinding(t *testing.T) {
	t.Setenv("STORAGE_S3_ENDPOINT", "minio:9000")
	t.Setenv("STORAGE_S3_MAX_RETRIES", "0")
	t.Setenv("STORAGE_DIRECTORY_PATH", "/dumps")
	t.Setenv("STORAGE_SSH_CONNECT_TIMEOUT", "1m")

	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	cfg := decodeStorage(t, v)

	s3cfg := toS3(cfg.S3)
	assert.Equal(t, "https://minio:9000", s3cfg.Endpoint)
	assert.Equal(t, 1, s3cfg.MaxRetries)
	assert.Equal(t, "/dumps", toDirectory(cfg.Directory).RootPath)
	assert.Equal(t, "1m0s", toSSH(cfg.SSH).ConnectTimeout.String())
}
