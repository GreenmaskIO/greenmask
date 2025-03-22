package tablegraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewTableLink(t *testing.T) {
	actual := NewTableLink(
		1,
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	expected := TableLink{
		ID: 1,
		table: common.Table{
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
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.TableID()
	require.Equal(t, 1, actual)
}

func TestTableLink_Table(t *testing.T) {
	tableLink := NewTableLink(
		1,
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.Table()
	require.Equal(t, common.Table{Schema: "test", Name: "test1"}, actual)
}

func TestTableLink_GetTableName(t *testing.T) {
	tableLink := NewTableLink(
		1,
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.FullTableName()
	require.Equal(t, "test.test1", actual)
}

func TestTableLink_Keys(t *testing.T) {
	tableLink := NewTableLink(
		1,
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.Keys()
	require.Equal(t, []Key{{Name: "test"}}, actual)
}

func TestTableLink_PolymorphicExpressions(t *testing.T) {
	tableLink := NewTableLink(
		1,
		common.Table{Schema: "test", Name: "test1"},
		[]Key{{Name: "test"}},
		[]string{"test"},
	)
	actual := tableLink.PolymorphicExpressions()
	require.Equal(t, []string{"test"}, actual)
}
