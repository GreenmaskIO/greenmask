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

package tabledriver

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/coretest"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// TestDriver_DecodeValueByColumnIdx_Signedness proves the column decode path is
// type-driven: an unsigned integer column yields uint64 for EVERY value (small
// and above-int64), while a signed column yields int64 — backed by the real
// canonical coretest codecs rather than a mock.
func TestDriver_DecodeValueByColumnIdx_Signedness(t *testing.T) {
	const maxUint64 = "18446744073709551615"

	table := &core.Table{
		Schema: "public",
		Name:   "t",
		Columns: []core.Column{
			{Idx: 0, Name: "signed", Type: core.Type{Name: coretest.TypeInt8, ID: coretest.TypeIDInt8, Class: core.TypeClassInt}},
			{Idx: 1, Name: "unsigned", Type: core.Type{Name: coretest.TypeInt8, ID: coretest.TypeIDInt8, Class: core.TypeClassInt, Unsigned: true}},
		},
	}

	ctx := validationcollector.WithCollector(context.Background(), validationcollector.NewCollector())
	d, err := New(ctx, coretest.New(), table, nil)
	require.NoError(t, err)

	tests := []struct {
		name string
		idx  int
		raw  []byte
		want any
	}{
		{"signed small to int64", 0, []byte("42"), int64(42)},
		{"unsigned small to uint64", 1, []byte("42"), uint64(42)},
		{"unsigned above int64 to uint64", 1, []byte(maxUint64), uint64(18446744073709551615)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := d.DecodeValueByColumnIdx(tc.idx, tc.raw)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}

	// A signed column cannot represent a value above int64 — strict typing makes
	// that a decode error, never a silent widening.
	_, err = d.DecodeValueByColumnIdx(0, []byte(maxUint64))
	require.Error(t, err)
}
