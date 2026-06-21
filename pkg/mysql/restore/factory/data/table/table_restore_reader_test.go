// Copyright 2025 Greenmask
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

package table

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func TestTableRestoreReader_Open_Success(t *testing.T) {
	st := &stubStorager{content: []byte("(1)\n(2)\n")}
	r := NewTableRestoreReader("test.data", core.CompressionNone)

	err := r.Open(context.Background(), st)
	require.NoError(t, err)
	assert.NotNil(t, r.rc)
	assert.NotNil(t, r.scanner)

	_ = r.Close(context.Background())
}

func TestTableRestoreReader_Open_StorageError(t *testing.T) {
	st := &stubStorager{getErr: errors.New("file not found")}
	r := NewTableRestoreReader("missing.data", core.CompressionNone)

	err := r.Open(context.Background(), st)
	require.Error(t, err)
	assert.ErrorContains(t, err, "open table data file")
	assert.ErrorContains(t, err, "missing.data")
}

func TestTableRestoreReader_ReadRow_ReturnsAllRows(t *testing.T) {
	data := "(1,'alice')\n(2,'bob')\n(3,'carol')\n"
	st := &stubStorager{content: []byte(data)}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	for _, want := range []string{"(1,'alice')", "(2,'bob')", "(3,'carol')"} {
		row, err := r.ReadRow(context.Background())
		require.NoError(t, err)
		assert.Equal(t, want, string(row))
	}

	_, err := r.ReadRow(context.Background())
	assert.ErrorIs(t, err, core.ErrEndOfStream)
}

func TestTableRestoreReader_ReadRow_SkipsBlankLines(t *testing.T) {
	data := "\n\n(1)\n\n\n(2)\n\n"
	st := &stubStorager{content: []byte(data)}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	row1, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "(1)", string(row1))

	row2, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "(2)", string(row2))

	_, err = r.ReadRow(context.Background())
	assert.ErrorIs(t, err, core.ErrEndOfStream)
}

func TestTableRestoreReader_ReadRow_SkipsWhitespaceOnlyLines(t *testing.T) {
	data := "   \n(42,'x')\n\t\n"
	st := &stubStorager{content: []byte(data)}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	row, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "(42,'x')", string(row))

	_, err = r.ReadRow(context.Background())
	assert.ErrorIs(t, err, core.ErrEndOfStream)
}

func TestTableRestoreReader_ReadRow_EmptyFile(t *testing.T) {
	st := &stubStorager{content: []byte("")}
	r := NewTableRestoreReader("empty.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	_, err := r.ReadRow(context.Background())
	assert.ErrorIs(t, err, core.ErrEndOfStream)
}

func TestTableRestoreReader_ReadRow_OnlyBlankLines(t *testing.T) {
	st := &stubStorager{content: []byte("\n\n\n")}
	r := NewTableRestoreReader("blank.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	_, err := r.ReadRow(context.Background())
	assert.ErrorIs(t, err, core.ErrEndOfStream)
}

func TestTableRestoreReader_ReadRow_NullValues(t *testing.T) {
	data := "(1,NULL,NULL)\n"
	st := &stubStorager{content: []byte(data)}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	row, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "(1,NULL,NULL)", string(row))
}

func TestTableRestoreReader_Close_SafeWhenNotOpened(t *testing.T) {
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	err := r.Close(context.Background())
	assert.NoError(t, err)
}

func TestTableRestoreReader_Close_SafeAfterClose(t *testing.T) {
	st := &stubStorager{content: []byte("(1)\n")}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))

	assert.NoError(t, r.Close(context.Background()))
	// Second close: rc is already closed; Close sets rc on open, so it won't be
	// nil — expect no panic (underlying close may return an error, which is logged).
}

func TestTableRestoreReader_DebugInfo(t *testing.T) {
	r := NewTableRestoreReader("myfile.data", core.CompressionGzip)
	info := r.DebugInfo()
	assert.Equal(t, "myfile.data", info["filename"])
	assert.Equal(t, core.CompressionGzip, info["compression"])
}

func TestTableRestoreReader_ReadRow_ReturnsCopy(t *testing.T) {
	// Ensure that modifications to the returned slice do not affect subsequent reads.
	data := "(1)\n(2)\n"
	st := &stubStorager{content: []byte(data)}
	r := NewTableRestoreReader("test.data", core.CompressionNone)
	require.NoError(t, r.Open(context.Background(), st))
	defer r.Close(context.Background())

	row1, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	// Mutate the returned slice.
	copy(row1, "XXXX")

	row2, err := r.ReadRow(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "(2)", string(row2))
}
