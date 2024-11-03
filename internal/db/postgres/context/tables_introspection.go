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
	"bytes"
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var typeSizes = map[string]int{
	"int2":   2,
	"int4":   4,
	"int8":   8,
	"float4": 4,
	"float8": 8,
}

func getTypeSizeByeName(name string) int {
	res, ok := typeSizes[name]
	if !ok {
		return -1
	}
	return res
}

func getTypeOidByName(name string, typeMap *pgtype.Map) toolkit.Oid {
	t, ok := typeMap.TypeForName(name)
	if !ok {
		return toolkit.Oid(t.OID)
	}
	return 0
}

func getColumnsConfig(ctx context.Context, tx pgx.Tx, oid toolkit.Oid, version int, excludeGenerated bool) ([]*toolkit.Column, error) {
	defaultTypeMap := pgtype.NewMap()
	var res []*toolkit.Column
	buf := bytes.NewBuffer(nil)
	err := TableColumnsQuery.Execute(
		buf,
		map[string]int{"Version": version},
	)
	if err != nil {
		return nil, fmt.Errorf("error templating TableColumnsQuery: %w", err)
	}
	rows, err := tx.Query(ctx, buf.String(), oid)
	if err != nil {
		return nil, fmt.Errorf("unable execute tableColumnQuery: %w", err)
	}
	defer rows.Close()

	idx := 0
	for rows.Next() {
		column := toolkit.Column{Idx: idx}
		if version >= 120000 {
			err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
				&column.NotNull, &column.Length, &column.Num, &column.TypeLength, &column.IsGenerated)
		} else {
			err = rows.Scan(&column.Name, &column.TypeOid, &column.TypeName,
				&column.NotNull, &column.Length, &column.Num, &column.TypeLength)
		}
		if err != nil {
			return nil, fmt.Errorf("cannot scan tableColumnQuery: %w", err)
		}
		// Skipping generated columns as they do not contain a real data
		if excludeGenerated && column.IsGenerated {
			continue
		}
		column.CanonicalTypeName = column.TypeName
		// Getting canonical type name if exists. For instance - PostgreSQL type Integer is alias for int4
		// (int4 - canonical type name)
		canonicalType, ok := defaultTypeMap.TypeForOID(uint32(column.TypeOid))
		if ok {
			column.CanonicalTypeName = canonicalType.Name
		}
		res = append(res, &column)
		idx++
	}

	return res, nil
}

func getTableConstraints(ctx context.Context, tx pgx.Tx, tableOid toolkit.Oid, version int) (
	[]toolkit.Constraint, error,
) {
	var constraints []toolkit.Constraint

	rows, err := tx.Query(ctx, TableConstraintsCommonQuery, tableOid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute TableConstraintsCommonQuery: %w", err)
	}
	defer rows.Close()

	// Common constraints discovering
	var pk *toolkit.PrimaryKey
	for rows.Next() {
		var c toolkit.Constraint
		var constraintOid toolkit.Oid
		var constraintName, constraintSchema, constraintDefinition, rtName, rtSchema string
		var constraintType rune
		var rtOid toolkit.Oid // rt - referenced table
		var constraintColumns, rtColumns []toolkit.AttNum

		err = rows.Scan(
			&constraintOid, &constraintName, &constraintSchema, &constraintType, &constraintColumns,
			&rtOid, &rtName, &rtSchema, &rtColumns, &constraintDefinition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

		switch constraintType {
		case 'f':
			// TODO: Recheck it
			c = &toolkit.ForeignKey{
				DefaultConstraintDefinition: toolkit.DefaultConstraintDefinition{
					Schema:     constraintSchema,
					Name:       constraintName,
					Oid:        constraintOid,
					Columns:    constraintColumns,
					Definition: constraintDefinition,
				},
			}
		case 'c':
			c = &toolkit.Check{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 'p':
			pk = toolkit.NewPrimaryKey(constraintSchema, constraintName, constraintDefinition, constraintOid, constraintColumns)
			c = pk
		case 'u':
			c = &toolkit.Unique{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 't':
			c = &toolkit.TriggerConstraint{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		case 'x':
			c = &toolkit.Exclusion{
				Schema:     constraintSchema,
				Name:       constraintName,
				Oid:        constraintOid,
				Columns:    constraintColumns,
				Definition: constraintDefinition,
			}
		default:
			return nil, fmt.Errorf("unknown constraint type %c", constraintType)
		}

		if err != nil {
			return nil, fmt.Errorf("cannot scan tableConstraintsQuery: %w", err)
		}
		constraints = append(constraints, c)
	}

	// Add FK references to PK
	buf := bytes.NewBuffer(nil)
	err = TablePrimaryKeyReferencesConstraintsQuery.Execute(
		buf,
		map[string]int{"Version": version},
	)
	if err != nil {
		return nil, fmt.Errorf("error templating TablePrimaryKeyReferencesConstraintsQuery: %w", err)
	}
	fkRows, err := tx.Query(ctx, buf.String(), tableOid)
	if err != nil {
		return nil, fmt.Errorf("cannot execute tableConstraintsQuery: %w", err)
	}
	defer fkRows.Close()

	for fkRows.Next() {
		var constraintOid, onTableOid toolkit.Oid
		var constraintName, constraintSchema, constraintDefinition, onTableSchema, onTableName string
		var constraintColumns []toolkit.AttNum

		err = fkRows.Scan(
			&constraintOid, &constraintSchema, &constraintName, &onTableOid,
			&onTableSchema, &onTableName, &constraintColumns, &constraintDefinition,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to build constraints list: %w", err)
		}

		pk.References = append(pk.References, &toolkit.LinkedTable{
			Oid:    onTableOid,
			Schema: onTableSchema,
			Name:   onTableName,
			Constraint: &toolkit.ForeignKey{
				DefaultConstraintDefinition: toolkit.DefaultConstraintDefinition{
					Schema:     constraintSchema,
					Name:       constraintName,
					Oid:        constraintOid,
					Columns:    constraintColumns,
					Definition: constraintDefinition,
				},
				ReferencedTable: toolkit.LinkedTable{
					Schema: onTableSchema,
					Name:   onTableName,
					Oid:    onTableOid,
				},
			},
		})
	}

	return constraints, nil
}

func escapeSubsetConds(conds []string) []string {
	var res []string
	for _, c := range conds {
		res = append(res, fmt.Sprintf(`( %s )`, c))
	}
	return res
}
