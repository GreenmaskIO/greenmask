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

func TestNewGraph(t *testing.T) {
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
	tableA := commonmodels.Table{
		Schema:     "test",
		Name:       "a",
		PrimaryKey: []string{"id"},
		References: nil,
	}

	tableB := commonmodels.Table{
		Schema:     "test",
		Name:       "b",
		PrimaryKey: []string{"id"},
		References: []commonmodels.Reference{
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

	tableC := commonmodels.Table{
		Schema:     "test",
		Name:       "c",
		PrimaryKey: []string{"id"},
		References: []commonmodels.Reference{
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

	tableD := commonmodels.Table{
		Schema:     "test",
		Name:       "d",
		PrimaryKey: []string{"id"},
		References: []commonmodels.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "d",
				Keys:             []string{"d_id"},
				IsNullable:       false,
			},
		},
	}

	tableF := commonmodels.Table{
		Schema:     "test",
		Name:       "f",
		PrimaryKey: []string{"id"},
		References: []commonmodels.Reference{
			{
				ReferencedSchema: "test",
				ReferencedName:   "b",
				Keys:             []string{"b_id"},
				IsNullable:       false,
			},
		},
	}

	tables := []commonmodels.Table{tableA, tableB, tableC, tableD, tableF}

	expected := Graph{
		Vertexes: tables,
		Graph: [][]Edge{
			// the edge a do not have any references
			nil,
			{
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
				{
					id:         1,
					isNullable: false,
					from: TableLink{
						ID:    1,
						table: tableB,
						keys: []Key{
							{
								Name: "f_id",
							},
						},
					},
					to: TableLink{
						ID:    4,
						table: tableF,
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
					id:         2,
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
					id:         3,
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
					id:         4,
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
			{
				// the edge d references d
				{
					id:         5,
					isNullable: false,
					from: TableLink{
						ID:    4,
						table: tableF,
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
					id:         3,
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
					id:         2,
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
				{
					id:         5,
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
						ID:    4,
						table: tableF,
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
					id:         4,
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
			{
				// the edge f references b
				{
					id:         1,
					isNullable: false,
					from: TableLink{
						ID:    4,
						table: tableF,
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
								Name: "f_id",
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
