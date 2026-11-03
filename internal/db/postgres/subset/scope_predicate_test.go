package subset

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func TestGenerateQueryForTablesPolymorphicScope(t *testing.T) {
	cases := []struct {
		name     string
		target   string
		exprs    []string
		nullable bool
	}{
		{
			name:   "control reference",
			target: "controls",
			exprs:  []string{"public.comments.commentable_type = 'Control'"},
		},
		{
			name:     "confirmation item reference",
			target:   "confirmation_items",
			exprs:    []string{"public.comments.commentable_type = 'ConfirmationItem'"},
			nullable: true,
		},
		{
			name:   "multiple discriminator conditions",
			target: "controls",
			exprs: []string{
				"public.comments.commentable_type = 'Control'",
				"public.comments.is_active",
			},
			nullable: true,
		},
		{
			name:     "ordinary nullable reference",
			target:   "controls",
			nullable: true,
		},
		{
			name:   "ordinary required reference",
			target: "controls",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			comments := &entries.Table{Table: &toolkit.Table{
				Schema: "public", Name: "comments", PrimaryKey: []string{"id"},
			}}
			target := &entries.Table{Table: &toolkit.Table{
				Schema: "public", Name: tc.target, PrimaryKey: []string{"id"},
			}}
			edge := NewEdge(0, 1, tc.nullable,
				NewTableLink(0, comments, NewKeysByColumn([]string{"commentable_id"}), tc.exprs),
				NewTableLink(1, target, NewKeysByColumn([]string{"id"}), nil),
			)
			condensedEdge := &CondensedEdge{originalEdge: edge}
			scope := &ScopeEdge{
				scopeId: 1, originalCondensedEdge: condensedEdge, isNullable: tc.nullable,
			}
			path := NewPath(0)
			path.scopeEdges[1] = []*CondensedEdge{condensedEdge}
			graph := &Graph{scc: []*Component{{
				tables: map[int]*entries.Table{0: comments},
			}}}

			query := graph.generateQueryForTables(path, scope)
			foreignKey := `"public"."comments"."commentable_id"`
			inPredicate := `((` + foreignKey + `) IN (SELECT "public"."` + tc.target + `"."id"`

			if len(tc.exprs) > 0 {
				require.Contains(t, query, "(NOT ("+strings.Join(tc.exprs, " AND ")+") OR "+inPredicate)
			} else {
				require.Contains(t, query, inPredicate)
				require.NotContains(t, query, "NOT (")
			}
			if tc.nullable {
				require.Contains(t, query, foreignKey+" IS NULL")
			} else {
				require.NotContains(t, query, foreignKey+" IS NULL")
			}
		})
	}
}

