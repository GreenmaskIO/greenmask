package subset

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestGenerateSelectAllColumns(t *testing.T) {
	table := &entries.Table{
		Table: &toolkit.Table{
			Schema: "public",
			Name:   "orders",
			Columns: []*toolkit.Column{
				{Name: "id"},
				{Name: "total", IsGenerated: true},
				{Name: "amount"},
			},
		},
	}

	require.Equal(
		t,
		`SELECT "public"."orders"."id", "public"."orders"."amount"`,
		generateSelectAllColumns(table),
	)
}

func TestGenerateSelectAllColumns_pt2(t *testing.T) {
	table := &entries.Table{
		Table: &toolkit.Table{
			Schema: "public",
			Name:   "orders",
			Columns: []*toolkit.Column{
				{Name: "total", IsGenerated: true},
			},
		},
	}

	require.Equal(t, `SELECT "public"."orders".*`, generateSelectAllColumns(table))
}
