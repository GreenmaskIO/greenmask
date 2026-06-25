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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/azure/azurite"

	"github.com/greenmaskio/greenmask/internal/storages"
)

func TestConfig_Validate_RequiresContainerAndAccount(t *testing.T) {
	t.Run("missing container", func(t *testing.T) {
		cfg := NewConfig()
		cfg.StorageAccount = "test-account"
		assert.Error(t, cfg.Validate())
	})

	t.Run("missing storage account", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Container = "test-container"
		assert.Error(t, cfg.Validate())
	})

	t.Run("valid input", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Container = "test-container"
		cfg.StorageAccount = "test-account"
		assert.NoError(t, cfg.Validate())
	})
}

func TestResolveAuth_AccessKey(t *testing.T) {
	cfg := NewConfig()
	cfg.AccessKey = "foo"
	at, sasToken, accessKey := resolveAuth(cfg)
	assert.Equal(t, authTypeAccessKey, at)
	assert.Empty(t, sasToken)
	assert.Equal(t, "foo", accessKey)
}

func TestResolveAuth_SASToken(t *testing.T) {
	t.Run("without leading question mark", func(t *testing.T) {
		cfg := NewConfig()
		cfg.SASToken = "foo"
		at, sasToken, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeSASToken, at)
		assert.Equal(t, "?foo", sasToken)
		assert.Empty(t, accessKey)
	})

	t.Run("with leading question mark", func(t *testing.T) {
		cfg := NewConfig()
		cfg.SASToken = "?foo"
		at, sasToken, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeSASToken, at)
		assert.Equal(t, "?foo", sasToken)
		assert.Empty(t, accessKey)
	})

	t.Run("access key takes precedence", func(t *testing.T) {
		cfg := NewConfig()
		cfg.AccessKey = "key"
		cfg.SASToken = "token"
		at, _, accessKey := resolveAuth(cfg)
		assert.Equal(t, authTypeAccessKey, at)
		assert.Equal(t, "key", accessKey)
	})
}

func TestResolveAuth_Default(t *testing.T) {
	cfg := NewConfig()
	at, sasToken, accessKey := resolveAuth(cfg)
	assert.Equal(t, authTypeNotSpecified, at)
	assert.Empty(t, sasToken)
	assert.Empty(t, accessKey)
}

func TestEndpointSuffix(t *testing.T) {
	tests := map[string]string{
		"AzureUSGovernmentCloud": "core.usgovcloudapi.net",
		"AzureChinaCloud":        "core.chinacloudapi.cn",
		"AzureGermanCloud":       "core.cloudapi.de",
		"AzurePublicCloud":       "core.windows.net",
		"":                       "core.windows.net",
		"SomethingElse":          "core.windows.net",
	}
	for env, want := range tests {
		assert.Equalf(t, want, getStorageEndpointSuffix(env), "environment %q", env)
	}
}

func TestConfig_Defaults(t *testing.T) {
	cfg := NewConfig()
	assert.Equal(t, defaultBufferSize, cfg.BufferSize)
	assert.Equal(t, defaultBuffers, cfg.MaxBuffers)
	assert.Equal(t, defaultTryTimeout, cfg.TryTimeout)
	assert.Equal(t, defaultEnvName, cfg.EnvironmentName)

	// Validate clamps below-minimum buffer sizes/counts to their minimums.
	cfg.Container = "test-container"
	cfg.StorageAccount = "test-account"
	cfg.BufferSize = 1
	cfg.MaxBuffers = 0
	require.NoError(t, cfg.Validate())
	assert.Equal(t, minBufferSize, cfg.BufferSize)
	assert.Equal(t, minBuffers, cfg.MaxBuffers)
}

func TestContainerBaseURL(t *testing.T) {
	t.Run("endpoint override is path-style", func(t *testing.T) {
		cfg := NewConfig()
		cfg.Endpoint = "http://127.0.0.1:10000/devstoreaccount1"
		cfg.Container = "greenmask-test"
		assert.Equal(t, "http://127.0.0.1:10000/devstoreaccount1/greenmask-test", containerBaseURL(cfg))
	})

	t.Run("subdomain form from environment name", func(t *testing.T) {
		cfg := NewConfig()
		cfg.StorageAccount = "acct"
		cfg.Container = "cont"
		assert.Equal(t, "https://acct.blob.core.windows.net/cont", containerBaseURL(cfg))
	})

	t.Run("explicit endpoint suffix wins over environment name", func(t *testing.T) {
		cfg := NewConfig()
		cfg.StorageAccount = "acct"
		cfg.Container = "cont"
		cfg.EnvironmentName = "AzureChinaCloud"
		cfg.EndpointSuffix = "core.windows.net"
		assert.Equal(t, "https://acct.blob.core.windows.net/cont", containerBaseURL(cfg))
	})
}

func TestBuildClientOptions(t *testing.T) {
	t.Run("try timeout maps to retry options", func(t *testing.T) {
		cfg := NewConfig()
		cfg.TryTimeout = 7
		opts := buildClientOptions(cfg)
		assert.Equal(t, 7*time.Minute, opts.Retry.TryTimeout)
		assert.Empty(t, opts.PerCallPolicies, "no api-version policy without an override")
	})

	t.Run("api version policy appended only when configured", func(t *testing.T) {
		cfg := NewConfig()
		cfg.BlobStoreAPIVersion = "2021-08-06"
		opts := buildClientOptions(cfg)
		require.Len(t, opts.PerCallPolicies, 1)
		p, ok := opts.PerCallPolicies[0].(*apiVersionPolicy)
		require.True(t, ok)
		assert.Equal(t, "2021-08-06", p.apiVersion)
	})
}

// recordingTransport captures the outgoing request and returns a canned 200.
type recordingTransport struct {
	captured *http.Request
}

func (rt *recordingTransport) Do(req *http.Request) (*http.Response, error) {
	rt.captured = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
		Header:     http.Header{},
		Request:    req,
	}, nil
}

func TestApiVersionPolicy(t *testing.T) {
	tests := []struct {
		name       string
		apiVersion string
		wantHeader string
	}{
		{"overrides x-ms-version when configured", "2021-08-06", "2021-08-06"},
		{"is a no-op when empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := &recordingTransport{}
			pipeline := runtime.NewPipeline(
				"test", "v1.0.0",
				runtime.PipelineOptions{PerCall: []policy.Policy{&apiVersionPolicy{apiVersion: tt.apiVersion}}},
				&policy.ClientOptions{Transport: rt},
			)
			req, err := runtime.NewRequest(context.Background(), http.MethodGet, "https://example.com")
			require.NoError(t, err)

			_, err = pipeline.Do(req)
			require.NoError(t, err)
			// The policy sets the header under the literal (non-canonical) key
			// so that it overrides the value the SDK sets under that same literal
			// key; read the raw map rather than Header.Get, which canonicalizes.
			got := rt.captured.Header["x-ms-version"] //nolint:staticcheck // SA1008: non-canonical key is intentional — must match the SDK's literal header key
			if tt.wantHeader == "" {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, []string{tt.wantHeader}, got)
			}
		})
	}
}

// TestNewStorage_AuthDispatch verifies that every auth method builds a usable
// container client (no network calls are made by client construction).
func TestNewStorage_AuthDispatch(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*Config)
	}{
		{"access key", func(c *Config) { c.AccessKey = azurite.AccountKey }},
		{"sas token", func(c *Config) { c.SASToken = "sig=abc&se=2030-01-01" }},
		{"default credential chain", func(c *Config) {}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.StorageAccount = azurite.AccountName
			cfg.Container = "cont"
			cfg.Endpoint = "http://127.0.0.1:10000/devstoreaccount1"
			tt.configure(cfg)

			st, err := NewStorage(context.Background(), cfg, "")
			require.NoError(t, err)
			require.NotNil(t, st)
			assert.NotNil(t, st.containerClient)
		})
	}
}

// --- Azurite test harness -------------------------------------------------
//
// A single Azurite container is shared across all storage tests (started
// lazily on first use, terminated in TestMain). Each test gets its own freshly
// created blob container via newTestStorage, so cases stay fully isolated.

const azuriteImage = "mcr.microsoft.com/azure-storage/azurite:latest"

var (
	azuriteOnce    sync.Once
	azuriteCtr     *azurite.Container
	azuriteBlobURL string
	azuriteErr     error

	containerCounter atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if azuriteCtr != nil {
		_ = azuriteCtr.Terminate(context.Background())
	}
	os.Exit(code)
}

// azuriteEndpoint lazily starts the shared Azurite container and returns the
// blob service URL. --skipApiVersionCheck lets the emulator accept the API
// version sent by the current Azure SDK.
func azuriteEndpoint(t *testing.T) string {
	t.Helper()
	azuriteOnce.Do(func() {
		ctx := context.Background()
		azuriteCtr, azuriteErr = azurite.Run(
			ctx, azuriteImage,
			azurite.WithEnabledServices(azurite.BlobService),
			testcontainers.WithCmdArgs("--skipApiVersionCheck"),
		)
		if azuriteErr != nil {
			return
		}
		azuriteBlobURL, azuriteErr = azuriteCtr.BlobServiceURL(ctx)
	})
	require.NoError(t, azuriteErr)
	return azuriteBlobURL
}

// newTestStorage returns a Storage backed by a unique, freshly created blob
// container in the shared Azurite emulator.
func newTestStorage(t *testing.T) *Storage {
	t.Helper()
	ctx := context.Background()
	endpoint := azuriteEndpoint(t)

	cfg := NewConfig()
	// Path-style endpoint includes the account name (Azurite well-known account).
	cfg.Endpoint = fmt.Sprintf("%s/%s", endpoint, azurite.AccountName)
	cfg.StorageAccount = azurite.AccountName
	cfg.AccessKey = azurite.AccountKey
	cfg.Container = fmt.Sprintf("test-%d", containerCounter.Add(1))

	st, err := NewStorage(ctx, cfg, "")
	require.NoError(t, err)
	_, err = st.containerClient.Create(ctx, nil)
	require.NoError(t, err)
	return st
}

// putObject is a small helper that writes content under key.
func putObject(t *testing.T, st *Storage, key string, content []byte) {
	t.Helper()
	require.NoError(t, st.PutObject(context.Background(), key, bytes.NewReader(content)))
}

// mustGet reads the object at key and returns its bytes.
func mustGet(t *testing.T, st *Storage, key string) []byte {
	t.Helper()
	r, err := st.GetObject(context.Background(), key)
	require.NoError(t, err)
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	require.NoError(t, err)
	return data
}

// rawListBlobs lists every blob in the container directly through the SDK,
// independent of the Storager implementation. It is used to cross-check that
// the storage actually holds (or no longer holds) the expected objects.
func rawListBlobs(t *testing.T, st *Storage) []string {
	t.Helper()
	ctx := context.Background()
	var names []string
	pager := st.containerClient.NewListBlobsFlatPager(nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		require.NoError(t, err)
		for _, b := range page.Segment.BlobItems {
			names = append(names, *b.Name)
		}
	}
	return names
}

func dirNames(dirs []storages.Storager) []string {
	names := make([]string, 0, len(dirs))
	for _, d := range dirs {
		names = append(names, d.Dirname())
	}
	return names
}

func mapKeys(m map[string][]byte) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func TestStorage_PutObject(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		content []byte
	}{
		{"root file", "file.txt", []byte("hello")},
		{"nested file", "dir/file.txt", []byte("nested")},
		{"deeply nested", "a/b/c/d.txt", []byte("deep")},
		{"leading slash key is trimmed", "/slash.txt", []byte("slash")},
		{"empty content", "empty.txt", []byte{}},
		{"binary content", "bin.dat", []byte{0x00, 0x01, 0x02, 0xff, 0xfe}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			putObject(t, st, tt.key, tt.content)
			assert.Equal(t, tt.content, mustGet(t, st, tt.key))
		})
	}

	t.Run("overwrite creates new version", func(t *testing.T) {
		st := newTestStorage(t)
		putObject(t, st, "v.txt", []byte("version-1"))
		putObject(t, st, "v.txt", []byte("version-2"))
		assert.Equal(t, []byte("version-2"), mustGet(t, st, "v.txt"))
		// the overwrite must not leave a duplicate blob behind
		assert.Equal(t, []string{"v.txt"}, rawListBlobs(t, st))
	})
}

func TestStorage_GetObject(t *testing.T) {
	tests := []struct {
		name        string
		putKey      string
		getKey      string
		wantContent []byte
		wantErr     error
	}{
		{"existing root", "f.txt", "f.txt", []byte("data"), nil},
		{"existing nested", "d/f.txt", "d/f.txt", []byte("nested"), nil},
		{"missing key", "", "missing.txt", nil, storages.ErrFileNotFound},
		{"leading slash matches put without slash", "f.txt", "/f.txt", []byte("data"), nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			if tt.putKey != "" {
				putObject(t, st, tt.putKey, tt.wantContent)
			}
			reader, err := st.GetObject(context.Background(), tt.getKey)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			defer func() { _ = reader.Close() }()
			data, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, tt.wantContent, data)
		})
	}
}

func TestStorage_Exists(t *testing.T) {
	tests := []struct {
		name     string
		put      []string
		checkKey string
		want     bool
	}{
		{"present root", []string{"a.txt"}, "a.txt", true},
		{"present nested", []string{"d/a.txt"}, "d/a.txt", true},
		{"absent", []string{"a.txt"}, "b.txt", false},
		{"absent in empty container", nil, "a.txt", false},
		{"prefix of existing key is not a blob", []string{"dir/a.txt"}, "dir", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			for _, k := range tt.put {
				putObject(t, st, k, []byte("x"))
			}
			got, err := st.Exists(context.Background(), tt.checkKey)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStorage_Stat(t *testing.T) {
	tests := []struct {
		name    string
		putKey  string
		statKey string
		wantErr bool
	}{
		{"existing root", "f.txt", "f.txt", false},
		{"existing nested", "d/f.txt", "d/f.txt", false},
		{"missing", "", "missing.txt", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			if tt.putKey != "" {
				putObject(t, st, tt.putKey, []byte("data"))
			}
			stat, err := st.Stat(tt.statKey)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.True(t, stat.Exist)
			assert.Equal(t, tt.statKey, stat.Name)
			assert.False(t, stat.LastModified.IsZero())
		})
	}
}

func TestStorage_ListDir(t *testing.T) {
	tests := []struct {
		name       string
		put        []string
		listPrefix string // "" lists the root storage, otherwise a relative SubStorage
		wantFiles  []string
		wantDirs   []string
	}{
		{
			name:      "mixed files and dirs at root",
			put:       []string{"a.txt", "b.txt", "d1/c.txt", "d2/e.txt"},
			wantFiles: []string{"a.txt", "b.txt"},
			wantDirs:  []string{"d1", "d2"},
		},
		{
			name:      "only files",
			put:       []string{"a.txt", "b.txt"},
			wantFiles: []string{"a.txt", "b.txt"},
			wantDirs:  nil,
		},
		{
			name:     "only dirs",
			put:      []string{"d1/a.txt", "d2/b.txt"},
			wantDirs: []string{"d1", "d2"},
		},
		{
			name: "empty container",
		},
		{
			name:       "nested listing via sub storage",
			put:        []string{"sub/x.txt", "sub/y.txt", "sub/deep/z.txt"},
			listPrefix: "sub",
			wantFiles:  []string{"x.txt", "y.txt"},
			wantDirs:   []string{"deep"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			for _, k := range tt.put {
				putObject(t, st, k, []byte("x"))
			}

			var target storages.Storager = st
			if tt.listPrefix != "" {
				target = st.SubStorage(tt.listPrefix, true)
			}

			files, dirs, err := target.ListDir(context.Background())
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantFiles, files)
			assert.ElementsMatch(t, tt.wantDirs, dirNames(dirs))
		})
	}
}

func TestStorage_Delete(t *testing.T) {
	tests := []struct {
		name     string
		put      []string
		del      []string
		wantGone []string
		wantKept []string
	}{
		{"single", []string{"a.txt", "b.txt"}, []string{"a.txt"}, []string{"a.txt"}, []string{"b.txt"}},
		{"multiple", []string{"a.txt", "b.txt", "c.txt"}, []string{"a.txt", "c.txt"}, []string{"a.txt", "c.txt"}, []string{"b.txt"}},
		{"non-existent is ignored", []string{"a.txt"}, []string{"ghost.txt"}, []string{"ghost.txt"}, []string{"a.txt"}},
		{"nested", []string{"d/a.txt", "d/b.txt"}, []string{"d/a.txt"}, []string{"d/a.txt"}, []string{"d/b.txt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := newTestStorage(t)
			for _, k := range tt.put {
				putObject(t, st, k, []byte("x"))
			}

			require.NoError(t, st.Delete(ctx, tt.del...))

			for _, k := range tt.wantGone {
				exists, err := st.Exists(ctx, k)
				require.NoError(t, err)
				assert.Falsef(t, exists, "expected %q to be gone", k)
			}
			for _, k := range tt.wantKept {
				exists, err := st.Exists(ctx, k)
				require.NoError(t, err)
				assert.Truef(t, exists, "expected %q to be kept", k)
			}
		})
	}
}

func TestStorage_DeleteAll(t *testing.T) {
	tests := []struct {
		name          string
		put           []string
		deletePrefix  string
		wantRemaining []string
	}{
		{
			name:          "prefix isolation leaves other prefixes intact",
			put:           []string{"books/a.txt", "books/b.txt", "users/u.txt"},
			deletePrefix:  "books",
			wantRemaining: []string{"users/u.txt"},
		},
		{
			name:          "nested prefix only",
			put:           []string{"books/sci/a.txt", "books/sci/b.txt", "books/hist/c.txt"},
			deletePrefix:  "books/sci",
			wantRemaining: []string{"books/hist/c.txt"},
		},
		{
			name:          "similarly named prefix is not affected",
			put:           []string{"data/a.txt", "data2/b.txt"},
			deletePrefix:  "data",
			wantRemaining: []string{"data2/b.txt"},
		},
		{
			name:          "delete everything from root",
			put:           []string{"a.txt", "d/b.txt", "d/e/c.txt"},
			deletePrefix:  "/",
			wantRemaining: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := newTestStorage(t)
			for _, k := range tt.put {
				putObject(t, st, k, []byte("x"))
			}

			require.NoError(t, st.DeleteAll(context.Background(), tt.deletePrefix))

			// Cross-check directly against the storage, not via ListDir.
			assert.ElementsMatch(t, tt.wantRemaining, rawListBlobs(t, st))
		})
	}
}

func TestStorage_SubStorage(t *testing.T) {
	t.Run("prefix computation", func(t *testing.T) {
		tests := []struct {
			name     string
			base     string
			sub      string
			relative bool
			wantCwd  string
		}{
			{"relative from root", "", "child", true, "child/"},
			{"relative nested", "parent/", "child", true, "parent/child/"},
			{"relative trims leading slash", "", "/child", true, "child/"},
			{"absolute replaces prefix", "parent/", "other", false, "other"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				st := &Storage{prefix: tt.base}
				sub := st.SubStorage(tt.sub, tt.relative)
				assert.Equal(t, tt.wantCwd, sub.GetCwd())
			})
		}
	})

	t.Run("round-trip", func(t *testing.T) {
		st := newTestStorage(t)
		sub := st.SubStorage("Sub1", true)
		content := []byte("sub-storage-payload")
		require.NoError(t, sub.PutObject(context.Background(), "test.txt", bytes.NewReader(content)))

		// readable through the sub storage
		reader, err := sub.GetObject(context.Background(), "test.txt")
		require.NoError(t, err)
		defer func() { _ = reader.Close() }()
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, content, data)

		// and at the full path through the root storage
		assert.Equal(t, content, mustGet(t, st, "Sub1/test.txt"))
	})
}

func TestStorage_GetCwd(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"", ""},
		{"foo", "foo/"},
		{"foo/bar", "foo/bar/"},
		{"/foo", "foo/"},
		{"foo/", "foo/"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			st := &Storage{prefix: fixPrefix(tt.prefix)}
			assert.Equal(t, tt.want, st.GetCwd())
		})
	}
}

func TestStorage_Dirname(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"", "."},
		{"foo", "foo"},
		{"foo/bar", "bar"},
		{"/foo/bar", "bar"},
	}
	for _, tt := range tests {
		t.Run(tt.prefix, func(t *testing.T) {
			st := &Storage{prefix: fixPrefix(tt.prefix)}
			assert.Equal(t, tt.want, st.Dirname())
		})
	}
}

// TestStorage_Integration exercises the full lifecycle against the emulator:
// create objects across independent prefixes, read them back, overwrite an
// object (new version), partially delete, then DeleteAll one prefix and verify
// the other prefix is untouched — cross-checked directly against the storage.
func TestStorage_Integration(t *testing.T) {
	ctx := context.Background()
	st := newTestStorage(t)

	// 1. create objects in two independent prefixes
	objects := map[string][]byte{
		"books/fiction/dune.txt":        []byte("dune v1"),
		"books/fiction/neuromancer.txt": []byte("neuromancer"),
		"books/history/rome.txt":        []byte("rome"),
		"users/alice/profile.txt":       []byte("alice"),
		"users/bob/profile.txt":         []byte("bob"),
	}
	for k, v := range objects {
		putObject(t, st, k, v)
	}
	// manual cross-check: every object is present in the storage
	assert.ElementsMatch(t, mapKeys(objects), rawListBlobs(t, st))

	// 2. read every object back and verify exists + content
	for k, v := range objects {
		exists, err := st.Exists(ctx, k)
		require.NoError(t, err)
		assert.Truef(t, exists, "expected %q to exist", k)
		assert.Equal(t, v, mustGet(t, st, k))
	}

	// 3. new version: overwriting an existing object replaces its content in place
	putObject(t, st, "books/fiction/dune.txt", []byte("dune v2"))
	assert.Equal(t, []byte("dune v2"), mustGet(t, st, "books/fiction/dune.txt"))
	assert.ElementsMatch(t, mapKeys(objects), rawListBlobs(t, st), "overwrite must not add a blob")

	// 4. partial delete: a single object goes, its sibling stays
	require.NoError(t, st.Delete(ctx, "books/fiction/neuromancer.txt"))
	exists, err := st.Exists(ctx, "books/fiction/neuromancer.txt")
	require.NoError(t, err)
	assert.False(t, exists)
	exists, err = st.Exists(ctx, "books/fiction/dune.txt")
	require.NoError(t, err)
	assert.True(t, exists)

	// 5. DeleteAll on the books prefix must not touch the users prefix
	require.NoError(t, st.DeleteAll(ctx, "books"))
	assert.ElementsMatch(t, []string{
		"users/alice/profile.txt",
		"users/bob/profile.txt",
	}, rawListBlobs(t, st))
	// users prefix is still fully readable
	assert.Equal(t, []byte("alice"), mustGet(t, st, "users/alice/profile.txt"))
	assert.Equal(t, []byte("bob"), mustGet(t, st, "users/bob/profile.txt"))

	// 6. DeleteAll everything from the root empties the storage
	require.NoError(t, st.DeleteAll(ctx, "/"))
	assert.Empty(t, rawListBlobs(t, st))
	files, dirs, err := st.ListDir(ctx)
	require.NoError(t, err)
	assert.Empty(t, files)
	assert.Empty(t, dirs)
}
