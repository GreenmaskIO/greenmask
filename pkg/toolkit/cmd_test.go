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

package toolkit

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCmd_init_stdin_closed_before_meta(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	require.NoError(t, w.Close())
	stdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = stdin
		_ = r.Close()
	}()

	c := NewCmd(NewTransformerDefinition("test", func(
		ctx context.Context, driver *Driver, parameters map[string]Parameterizer,
	) (Transformer, ValidationWarnings, error) {
		return nil, nil, nil
	}))

	start := time.Now()
	_, _, _, err = c.init(context.Background())
	require.ErrorIs(t, err, io.EOF)
	// must fail on EOF instead of spinning until the metadata timeout
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestCmd_init_meta_without_new_line_before_eof(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	_, err = w.Write([]byte(`{}`))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	stdin := os.Stdin
	os.Stdin = r
	defer func() {
		os.Stdin = stdin
		_ = r.Close()
	}()

	c := NewCmd(NewTransformerDefinition("test", func(
		ctx context.Context, driver *Driver, parameters map[string]Parameterizer,
	) (Transformer, ValidationWarnings, error) {
		return nil, nil, nil
	}))

	_, _, _, err = c.init(context.Background())
	// the line is read and parsed, it fails later on the empty table
	require.ErrorContains(t, err, "empty Table")
}
