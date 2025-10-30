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

func TestNewTableLink(t *testing.T) {
	actual := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	expected := TableLink{
		ID: 1,
		table: commonmodels.Table{
			Schema: "test",
			Name:   "test1",
		},
		keys:                   []Key{{Name: "test"}},
		polymorphicExpressions: []string{"test"},
	}
	require.Equal(t, expected, actual)
}

func TestTableLink_Index(t *testing.T) {
	tableLink := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.TableID()
	require.Equal(t, 1, actual)
}

func TestTableLink_Table(t *testing.T) {
	tableLink := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.Table()
	require.Equal(t, commonmodels.Table{Schema: "test", Name: "test1"}, actual)
}

func TestTableLink_GetTableName(t *testing.T) {
	tableLink := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.FullTableName()
	require.Equal(t, "test.test1", actual)
}

func TestTableLink_Keys(t *testing.T) {
	tableLink := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.Keys()
	require.Equal(t, []Key{{Name: "test"}}, actual)
}

func TestTableLink_PolymorphicExpressions(t *testing.T) {
	tableLink := NewTableLink(
		1,
		commonmodels.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.PolymorphicExpressions()
	require.Equal(t, []string{"test"}, actual)
}
