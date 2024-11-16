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
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/entries"
	"github.com/greenmaskio/greenmask/internal/db/postgres/subset"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const defaultTransformerCostMultiplier = 0.03

// RuntimeContext - describes current runtime behaviour according to the config and schema objects
type RuntimeContext struct {
	// Types - list of custom types that are used in DB schema
	Types []*toolkit.Type
	// DataSectionObjects - list of objects to dump in data-section. There are sequences, Tables and large objects
	DataSectionObjects []entries.Entry
	// DataSectionObjectsToValidate - list of objects to validate in data-section
	DataSectionObjectsToValidate []entries.Entry
	// Warnings - list of occurred ValidationWarning during validation and config building
	Warnings toolkit.ValidationWarnings
	// Registry - registry of all the registered transformers definition
	Registry *transformersUtils.TransformerRegistry
	// TypeMap - map of registered types including custom types. It's common for the whole runtime
	TypeMap *pgtype.Map
	// DatabaseSchema - list of Tables with columns - required for schema diff checking
	DatabaseSchema toolkit.DatabaseSchema
	// Graph - graph of Tables with dependencies
	Graph *subset.Graph
}

// NewRuntimeContext - creating new runtime context.
// TODO: Recheck it is working properly. In a few cases (stages such as parameters building, schema validation) if
//
//	warnings are fatal procedure must be terminated immediately due to lack of objects required on the next step
func NewRuntimeContext(
	ctx context.Context, tx pgx.Tx, cfg *domains.Dump,
	r *transformersUtils.TransformerRegistry,
	vr []*domains.VirtualReference, version int,
) (*RuntimeContext, error) {
	var warnings toolkit.ValidationWarnings

	// Get salt from env and set it to the context
	ctx, err := withSalt(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot set salt: %w", err)
	}
	// Get custom types used in Tables and register them in the type map
	typeMap := tx.Conn().TypeMap()
	types, err := buildTypeMap(ctx, tx, typeMap)
	if err != nil {
		return nil, fmt.Errorf("cannot build type map: %w", err)
	}

	// Get list of entries (Tables, sequences, blobs) from the database
	tables, sequences, blobs, err := getDumpObjects(ctx, version, tx, &cfg.PgDumpOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot get Tables: %w", err)
	}

	vrWarns := validateVirtualReferences(vr, tables)
	warnings = append(warnings, vrWarns...)
	if len(vrWarns) > 0 {
		// if there are any warnings, we shouldn't use them in the graph build
		vr = nil
	}

	graph, err := subset.NewGraph(ctx, tx, slices.Clone(tables), vr)
	if err != nil {
		return nil, fmt.Errorf("error creating graph: %w", err)
	}
	if hasSubset(tables) {
		// If table has subset the restoration must be in the topological order
		// The Tables must be dumped one by one
		if err = subset.SetSubsetQueries(graph); err != nil {
			return nil, fmt.Errorf("cannot set subset queries: %w", err)
		}
		debugQueries(tables)
	} else {
		// if there are no subset Tables, we can sort them by size and transformation costs
		// TODO: Implement Tables ordering for subsetted Tables as well
		scoreTablesEntriesAndSort(tables)
	}

	buildWarns, err := validateAndBuildEntriesConfig(
		ctx, tx, tables, typeMap, cfg, r, version, types, graph,
	)
	if err != nil {
		return nil, fmt.Errorf("cannot validate and build table config: %w", err)
	}
	warnings = append(warnings, buildWarns...)
	if buildWarns.IsFatal() {
		return &RuntimeContext{
			Warnings: warnings,
		}, nil
	}

	var dataSectionObjects []entries.Entry
	for _, seq := range sequences {
		dataSectionObjects = append(dataSectionObjects, seq)
	}
	for _, table := range tables {
		dataSectionObjects = append(dataSectionObjects, table)
	}
	if blobs != nil {
		dataSectionObjects = append(dataSectionObjects, blobs)
	}

	// Generate list of Tables that might be validated during the validate command call
	var dataSectionObjectsToValidate []entries.Entry
	for _, item := range dataSectionObjects {
		if t, ok := item.(*entries.Table); ok && len(t.TransformersContext) > 0 {
			dataSectionObjectsToValidate = append(dataSectionObjectsToValidate, t)
		}
	}

	schema, err := getDatabaseSchema(ctx, tx, &cfg.PgDumpOptions, version)
	if err != nil {
		return nil, fmt.Errorf("cannot get database schema: %w", err)
	}

	return &RuntimeContext{
		Types:                        types,
		DataSectionObjects:           dataSectionObjects,
		Warnings:                     warnings,
		Registry:                     r,
		DatabaseSchema:               schema,
		Graph:                        graph,
		DataSectionObjectsToValidate: dataSectionObjectsToValidate,
	}, nil
}

func (rc *RuntimeContext) IsFatal() bool {
	return rc.Warnings.IsFatal()
}

func scoreTablesEntriesAndSort(tables []*entries.Table) {
	for _, t := range tables {
		transformersCount := float64(len(t.TransformersContext))
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

func debugQueries(tables []*entries.Table) {
	for _, t := range tables {
		if t.Query == "" {
			continue
		}
		log.Debug().
			Str("Schema", t.Schema).
			Str("Table", t.Name).
			Msg("Debug query")
		log.Logger.Println(t.Query)
	}
}

func withSalt(ctx context.Context) (context.Context, error) {
	var salt []byte
	saltHex := os.Getenv("GREENMASK_GLOBAL_SALT")
	if saltHex != "" {
		salt = make([]byte, hex.DecodedLen(len(saltHex)))
		_, err := hex.Decode(salt, []byte(saltHex))
		if err != nil {
			return nil, fmt.Errorf("error decoding salt from hex: %w", err)
		}
	}
	return utils.WithSalt(ctx, salt), nil
}

func buildTypeMap(ctx context.Context, tx pgx.Tx, tm *pgtype.Map) ([]*toolkit.Type, error) {
	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("cannot discover types: %w", err)
	}
	if len(types) > 0 {
		toolkit.TryRegisterCustomTypes(tm, types, true)
	}
	return types, nil

}
