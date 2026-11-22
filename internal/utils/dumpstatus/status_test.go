// Copyright 2026 Greenmask
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

package dumpstatus

import (
	"context"
	"strings"
	"testing"

	"filippo.io/age"
	"github.com/greenmaskio/storages/inmemory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/cmd"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages/encryption"
)

func TestGetStatus(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name  string
		files map[string]string
		want  string
	}{
		{name: "empty", files: nil, want: UnknownOrFailedStatusName},
		{name: "legacy done", files: map[string]string{"metadata.json": "{}"}, want: DoneStatusName},
		{name: "done", files: map[string]string{"heartbeat": "done", "metadata.json": "{}"}, want: DoneStatusName},
		{name: "done without metadata", files: map[string]string{"heartbeat": "done"}, want: UnknownOrFailedStatusName},
		{name: "empty heartbeat", files: map[string]string{"heartbeat": ""}, want: FailedStatusName},
		{name: "in progress", files: map[string]string{"heartbeat": "in-progress"}, want: InProgressStatusName},
		{name: "unknown heartbeat", files: map[string]string{"heartbeat": "x"}, want: UnknownOrFailedStatusName},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := inmemory.New("")
			for name, body := range tt.files {
				require.NoError(t, st.PutObject(ctx, name, strings.NewReader(body)))
			}
			got, err := GetStatus(ctx, st)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGetStatus_encryptedWithoutKey(t *testing.T) {
	ctx := context.Background()
	key, err := age.GenerateX25519Identity()
	require.NoError(t, err)
	root := inmemory.New("")
	writer, err := encryption.NewStorage(root, domains.EncryptionConfig{Key: key.String()}, cmd.HeartBeatFileName)
	require.NoError(t, err)
	st, err := writer.SubStorage("100", true)
	require.NoError(t, err)
	require.NoError(t, st.PutObject(ctx, "metadata.json", strings.NewReader(`{"description":"x"}`)))
	require.NoError(t, st.PutObject(ctx, "heartbeat", strings.NewReader("done")))

	reader, err := encryption.NewStorage(root, domains.EncryptionConfig{}, cmd.HeartBeatFileName)
	require.NoError(t, err)
	dump, err := reader.SubStorage("100", true)
	require.NoError(t, err)

	status, err := GetStatus(ctx, dump)
	require.NoError(t, err)
	assert.Equal(t, DoneStatusName, status)
	_, err = GetMetadata(ctx, dump)
	require.Error(t, err, "without the key, the metadata is ciphertext")
}
