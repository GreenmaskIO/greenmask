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

package tablegraph

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func TestNewEdge(t *testing.T) {
	edgeID := 2
	isNullable := true
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	actual := NewEdge(edgeID, isNullable, leftLink, rightLink)
	expected := Edge{
		id:         edgeID,
		isNullable: true,
		from:       leftLink,
		to:         rightLink,
	}
	require.Equal(t, expected, actual)
}

func TestEdge_From(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.From()
	require.Equal(t, leftLink, actual)
}

func TestEdge_To(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.To()
	require.Equal(t, rightLink, actual)
}

func TestEdge_IsNullable(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.IsNullable()
	require.Equal(t, true, actual)
}

func TestEdge_ID(t *testing.T) {
	leftLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	rightLink := NewTableLink(1, commonmodels.Table{}, nil, nil)
	edge := NewEdge(2, true, leftLink, rightLink)
	actual := edge.ID()
	require.Equal(t, 2, actual)
}
