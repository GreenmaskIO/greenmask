package subset

import (
	"fmt"
	"slices"
	"strings"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
)

type cteQuery struct {
	items []*cteItem
	c     *Component
}

func newCteQuery(c *Component) *cteQuery {
	return &cteQuery{
		c: c,
	}
}

func (c *cteQuery) addItem(name, query string) {
	c.items = append(c.items, &cteItem{
		name:  name,
		query: query,
	})
}

func (c *cteQuery) generateQuery(targetTable *entries.Table) string {
	var queries []string
	var excludedCteQueries []string
	if len(c.c.groupedCycles) > 1 {
		panic("FIXME: found more than one grouped cycle")
	}
	for _, edge := range c.c.cycles[0] {
		if edge.from.table.Oid == targetTable.Oid {
			continue
		}
		excludedCteQuery := fmt.Sprintf("%s__%s__ids", edge.from.table.Schema, edge.from.table.Name)
		excludedCteQueries = append(excludedCteQueries, excludedCteQuery)
	}

	for _, item := range c.items {
		if slices.Contains(excludedCteQueries, item.name) {
			continue
		}
		queries = append(queries, fmt.Sprintf(" %s AS (%s)", item.name, item.query))
	}
	var leftTableKeys, rightTableKeys []string
	rightTableName := fmt.Sprintf("%s__%s__ids", targetTable.Schema, targetTable.Name)
	for _, key := range targetTable.PrimaryKey {
		leftTableKeys = append(leftTableKeys, fmt.Sprintf(`"%s"."%s"."%s"`, targetTable.Schema, targetTable.Name, key))
		rightTableKeys = append(rightTableKeys, fmt.Sprintf(`"%s"."%s"`, rightTableName, key))
	}

	resultingQuery := fmt.Sprintf(
		`SELECT * FROM "%s"."%s" WHERE %s IN (SELECT %s FROM "%s")`,
		targetTable.Schema,
		targetTable.Name,
		fmt.Sprintf("(%s)", strings.Join(leftTableKeys, ",")),
		strings.Join(rightTableKeys, ","),
		rightTableName,
	)
	res := fmt.Sprintf("WITH RECURSIVE %s %s", strings.Join(queries, ","), resultingQuery)
	return res
}

type cteItem struct {
	name  string
	query string
}
