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

package transformers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// runCmdTransformer transforms one row via Cmd and returns the record; Done errors are ignored
// because the process is terminated with SIGTERM.
func runCmdTransformer(
	t *testing.T, columns []string, row string, params map[string]toolkit.ParamsValue,
) *toolkit.Record {
	t.Helper()
	driver, record := getDriverAndRecordByColumns(columns, row)
	tc, warnings, err := CmdTransformerDefinition.Instance(context.Background(), driver, params, nil, "", false)
	require.NoError(t, err)
	require.Empty(t, warnings)
	require.NoError(t, tc.Transformer.Init(context.Background()))
	defer func() {
		_ = tc.Transformer.Done(context.Background())
	}()
	r, err := tc.Transformer.Transform(context.Background(), record)
	require.NoError(t, err)
	return r
}
