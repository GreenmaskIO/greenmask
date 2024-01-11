// Copyright 2023 Greenmask
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

package context

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

func getCustomTypesUsedInTables(ctx context.Context, tx pgx.Tx) ([]*toolkit.Type, error) {
	var res []*toolkit.Type
	rows, err := tx.Query(ctx, CustomTypesWithTypeChainQuery)
	if err != nil {
		return nil, fmt.Errorf("unable execute CustomTypesUsedInTablesQuery: %w", err)
	}
	defer rows.Close()

	// Collect all types and find domains with constraint
	var domainsWithConstraint []*toolkit.Type
	for rows.Next() {
		t := &toolkit.Type{}
		var hasDomainConstraint bool
		err = rows.Scan(&t.Oid, &t.ChainOids, &t.ChainNames, &t.Schema, &t.Name, &t.Length, &t.Kind,
			&t.ComposedRelation, &t.ElementType, &t.ArrayType, &t.NotNull, &t.BaseType,
			&hasDomainConstraint)
		if err != nil {
			return nil, fmt.Errorf("cannot scan CustomTypesUsedInTablesQuery: %w", err)
		}
		if hasDomainConstraint {
			domainsWithConstraint = append(domainsWithConstraint, t)
		}
		if t.Kind == 'd' && len(t.ChainOids) > 0 {
			t.RootBuiltInTypeOid = t.ChainOids[len(t.ChainOids)-1]
			t.RootBuiltInTypeName = t.ChainNames[len(t.ChainNames)-1]
		}
		_, exists := tx.Conn().TypeMap().TypeForOID(uint32(t.Oid))
		if !exists {
			res = append(res, t)
		}
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
