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
	"encoding/hex"
	"fmt"
	"os"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/subset"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const defaultTransformerCostMultiplier = 0.03

// RuntimeContext - describes current runtime behaviour according to the config and schema objects
type RuntimeContext struct {
	// Tables - map of build tables with toolkit that was wrapped into dump.Entry
	Tables map[toolkit.Oid]*entries.Table
	// Types - list of custom types that are used in DB schema
	Types []*toolkit.Type
	// DataSectionObjects - list of objects to dump in data-section. There are sequences, tables and large objects
	DataSectionObjects []entries.Entry
	// Warnings - list of occurred ValidationWarning during validation and config building
	Warnings toolkit.ValidationWarnings
	// Registry - registry of all the registered transformers definition
	Registry *transformersUtils.TransformerRegistry
	// TypeMap - map of registered types including custom types. It's common for the whole runtime
	TypeMap *pgtype.Map
	// DatabaseSchema - list of tables with columns - required for schema diff checking
	DatabaseSchema toolkit.DatabaseSchema
	// Graph - graph of tables with dependencies
	Graph *subset.Graph
}

// NewRuntimeContext - creating new runtime context.
// TODO: Recheck it is working properly. In a few cases (stages such as parameters building, schema validation) if
//
//	warnings are fatal procedure must be terminated immediately due to lack of objects required on the next step
func NewRuntimeContext(
	ctx context.Context, tx pgx.Tx, cfg []*domains.Table, r *transformersUtils.TransformerRegistry, opt *pgdump.Options,
	version int,
) (*RuntimeContext, error) {
	var salt []byte
	saltHex := os.Getenv("GREENMASK_GLOBAL_SALT")
	if saltHex != "" {
		salt = make([]byte, hex.DecodedLen(len(saltHex)))
		_, err := hex.Decode(salt, []byte(saltHex))
		if err != nil {
			return nil, fmt.Errorf("error decoding salt from hex: %w", err)
		}
	}
	ctx = context.WithValue(ctx, "salt", salt)

	typeMap := tx.Conn().TypeMap()
	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("cannot discover types: %w", err)
	}
	if len(types) > 0 {
		toolkit.TryRegisterCustomTypes(typeMap, types, true)
	}

	tables, warnings, err := validateAndBuildTablesConfig(ctx, tx, typeMap, cfg, r, version, types)
	if err != nil {
		return nil, fmt.Errorf("cannot validate and build table config: %w", err)
	}

	tablesEntries, sequenceEntries, blobEntries, err := getDumpObjects(ctx, version, tx, opt, tables)
	if err != nil {
		return nil, fmt.Errorf("cannot build dump object list: %w", err)
	}

	schema, err := getDatabaseSchema(ctx, tx, opt, version)
	if err != nil {
		return nil, fmt.Errorf("cannot get database schema: %w", err)
	}

	graph, err := subset.NewGraph(ctx, tx, tablesEntries)
	if err != nil {
		return nil, fmt.Errorf("error creating graph: %w", err)
	}
	if hasSubset(tablesEntries) {
		// If table has subset the restoration must be in the topological order
		// The tables must be dumped one by one
		if err = subset.SetSubsetQueries(graph); err != nil {
			return nil, fmt.Errorf("cannot set subset queries: %w", err)
		}

	} else {
		// if there are no subset tables, we can sort them by size and transformation costs
		// TODO: Implement tables ordering for subsetted tables as well
		scoreTablesEntriesAndSort(tablesEntries, cfg)
	}

	var dataSectionObjects []entries.Entry
	for _, seq := range sequenceEntries {
		dataSectionObjects = append(dataSectionObjects, seq)
	}
	for _, table := range tablesEntries {
		dataSectionObjects = append(dataSectionObjects, table)
	}
	if blobEntries != nil {
		dataSectionObjects = append(dataSectionObjects, blobEntries)
	}

	return &RuntimeContext{
		Tables:             tables,
		Types:              types,
		DataSectionObjects: dataSectionObjects,
		Warnings:           warnings,
		Registry:           r,
		DatabaseSchema:     schema,
		Graph:              graph,
	}, nil
}

func (rc *RuntimeContext) IsFatal() bool {
	return rc.Warnings.IsFatal()
}

func scoreTablesEntriesAndSort(tables []*entries.Table, cfg []*domains.Table) {
	for _, t := range tables {
		var transformersCount float64
		idx := slices.IndexFunc(cfg, func(table *domains.Table) bool {
			return table.Name == t.Name && table.Schema == t.Schema
		})
		if idx != -1 {
			transformersCount = float64(len(cfg[idx].Transformers))
		}

		// scores = relSize + (realSize * 0.03 * transformersCount)
		t.Scores = t.Size + int64(float64(t.Size)*defaultTransformerCostMultiplier*transformersCount)
	}

	slices.SortFunc(tables, func(a, b *entries.Table) int {
		if a.Scores > b.Scores {
			return -1
		} else if a.Scores < b.Scores {
			return 1
		}
		return 0
	})

}

func hasSubset(tables []*entries.Table) bool {
	return slices.ContainsFunc(tables, func(table *entries.Table) bool {
		return len(table.SubsetConds) > 0
	})
}
