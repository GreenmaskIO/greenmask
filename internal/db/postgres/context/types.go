package context

import (
	"context"
	"fmt"

	"github.com/GreenmaskIO/greenmask/pkg/toolkit/transformers"
)

func getCustomTypesUsedInTables(ctx context.Context, tx pgx.Tx) ([]*transformers.Type, error) {
	var res []*transformers.Type
	rows, err := tx.Query(ctx, CustomTypesUsedInTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("unable execute CustomTypesUsedInTablesQuery: %w", err)
	}
	defer rows.Close()

	// Collect all types and find domains with constraint
	var domainsWithConstraint []*transformers.Type
	for rows.Next() {
		t := &transformers.Type{}
		var hasDomainConstraint bool
		if err = rows.Scan(&t.Oid, &t.Schema, &t.Name, &t.Length, &t.Kind,
			&t.ComposedRelation, &t.ElementType, &t.ArrayType, &t.NotNull, &t.BaseType,
			&hasDomainConstraint,
		); err != nil {
			return nil, fmt.Errorf("cannot scan CustomTypesUsedInTablesQuery: %w", err)
		}
		if hasDomainConstraint {
			domainsWithConstraint = append(domainsWithConstraint, t)
		}
		res = append(res, t)
	}

	// Assign domain constraints
	for _, t := range domainsWithConstraint {
		c := &transformers.Check{}

		row := tx.QueryRow(ctx, DomainConstraintsQuery, t.Oid)
		err = row.Scan(&c.Oid, &c.Schema, &c.Name, &c.Definition)
		if err != nil {
			return nil, fmt.Errorf("may be bug: expected domain constraint but was not fund: %w", err)
		}
		t.Check = c
	}

	return res, nil
}
