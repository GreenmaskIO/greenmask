package context

import (
	"context"
	"fmt"

	toolkit "github.com/GreenmaskIO/greenmask/internal/toolkit/transformers"
)

func getCustomTypesUsedInTables(ctx context.Context, tx pgx.Tx) ([]*toolkit.Type, error) {
	var res []*toolkit.Type
	rows, err := tx.Query(ctx, CustomTypesUsedInTablesQuery)
	if err != nil {
		return nil, fmt.Errorf("unable execute CustomTypesUsedInTablesQuery: %w", err)
	}
	defer rows.Close()

	// Collect all types and find domains with constraint
	var domainsWithConstraint []*toolkit.Type
	for rows.Next() {
		t := &toolkit.Type{}
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
		c := &toolkit.Check{}

		row := tx.QueryRow(ctx, DomainConstraintsQuery, t.Oid)
		err = row.Scan(&c.Oid, &c.Schema, &c.Name, &c.Definition)
		if err != nil {
			return nil, fmt.Errorf("may be bug: expected domain constraint but was not fund: %w", err)
		}
		t.Check = c
	}

	return res, nil
}
