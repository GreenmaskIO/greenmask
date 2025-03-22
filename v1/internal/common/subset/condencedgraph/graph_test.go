package condencedgraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/subset/tablegraph"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewGraph(t *testing.T) {
	g := getGraph(t)

	cg := NewGraph(g)
	require.NotNil(t, cg)
}

func getGraph(t *testing.T) tablegraph.Graph {
	t.Helper()
	/*
			There are 3 tables in the graph: a, b, c

			The graph should be represented as follows:

					f -          -- (F -> B  has a cycle)
					^  |
					|-<
				a <- b <- c
				|
				 <- c

				d --             -- D has a cycle
				^	|
		        |----
	*/
	tableA := common.Table{
		Schema:     "test",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: nil,
	}

	tableB := common.Table{
		Schema:     "test",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "a",
				Keys:             []string{"a_id"},
				IsNullable:       false,
			},
			{
				ReferencedSchema: "test",
				ReferencedName:   "f",
				Keys:             []string{"f_id"},
				IsNullable:       false,
			},
		},
	}

	tableC := common.Table{
		Schema:     "test",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "b",
				Keys:             []string{"b_id"},
				IsNullable:       false,
			},
			{
				ReferencedSchema: "test",
				ReferencedName:   "a",
				Keys:             []string{"a_id"},
				IsNullable:       false,
			},
		},
	}

	tableD := common.Table{
		Schema:     "test",
		Name:       "d",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "d",
				Keys:             []string{"d_id"},
				IsNullable:       false,
			},
		},
	}

	tableF := common.Table{
		Schema:     "test",
		Name:       "f",
		PrimaryKey: []string{"id"},
		References: []models.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "b",
				Keys:             []string{"b_id"},
				IsNullable:       false,
			},
		},
	}

	tables := []common.Table{tableA, tableB, tableC, tableD, tableF}

	g, err := tablegraph.NewGraph(tables)
	require.NoError(t, err)
	return g
}
