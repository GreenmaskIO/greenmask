package tablegraph

import (
	"github.com/greenmaskio/greenmask/v1/internal/common"
	"github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewGraph(t *testing.T) {
	/*
			There are 3 tables in the graph: a, b, c

			The graph should be represented as follows:

				a <- b <- c
				|
				 <- c

				d --    -- D has a cycle
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

	tables := []common.Table{tableA, tableB, tableC, tableD}

	expected := Graph{
		Vertexes: tables,
		Graph: [][]Edge{
			// the edge a do not have any references
			nil,
			{
				// the edge b references a
				{
					id:         0,
					isNullable: false,
					from: TableLink{
						ID:    1,
						table: tableB,
						keys: []Key{
							{
								Name: "a_id",
							},
						},
					},
					to: TableLink{
						ID:    0,
						table: tableA,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
				},
			},
			{
				// the edge c references b and a
				{
					id:         1,
					isNullable: false,
					from: TableLink{
						ID:    2,
						table: tableC,
						keys: []Key{
							{
								Name: "b_id",
							},
						},
					},
					to: TableLink{
						ID:    1,
						table: tableB,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
				},
				{
					id:         2,
					isNullable: false,
					from: TableLink{
						ID:    2,
						table: tableC,
						keys: []Key{
							{
								Name: "a_id",
							},
						},
					},
					to: TableLink{
						ID:    0,
						table: tableA,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
				},
			},
			{
				// the edge d references d
				{
					id:         3,
					isNullable: false,
					from: TableLink{
						ID:    3,
						table: tableD,
						keys: []Key{
							{
								Name: "d_id",
							},
						},
					},
					to: TableLink{
						ID:    3,
						table: tableD,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
				},
			},
		},

		TransposedGraph: [][]Edge{
			{
				// the edge c references b and a
				{
					id:         0,
					isNullable: false,
					from: TableLink{
						ID:    0,
						table: tableA,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
					to: TableLink{
						ID:    1,
						table: tableB,
						keys: []Key{
							{
								Name: "a_id",
							},
						},
					},
				},
				{
					id:         2,
					isNullable: false,
					from: TableLink{
						ID:    0,
						table: tableA,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
					to: TableLink{
						ID:    2,
						table: tableC,
						keys: []Key{
							{
								Name: "a_id",
							},
						},
					},
				},
			},
			{
				// the edge b references a
				{
					id:         1,
					isNullable: false,
					from: TableLink{
						ID:    1,
						table: tableB,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
					to: TableLink{
						ID:    2,
						table: tableC,
						keys: []Key{
							{
								Name: "b_id",
							},
						},
					},
				},
			},
			// the edge a do not have any references
			nil,
			{
				// the edge d references d
				{
					id:         3,
					isNullable: false,
					from: TableLink{
						ID:    3,
						table: tableD,
						keys: []Key{
							{
								Name: "id",
							},
						},
					},
					to: TableLink{
						ID:    3,
						table: tableD,
						keys: []Key{
							{
								Name: "d_id",
							},
						},
					},
				},
			},
		},
	}

	actual, err := NewGraph(tables)
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}
