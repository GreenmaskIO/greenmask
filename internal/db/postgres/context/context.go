package context

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	toolkit2 "github.com/greenmaskio/greenmask/pkg/toolkit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// RuntimeContext - describes current runtime behaviour according to the config and schema objects
type RuntimeContext struct {
	// Tables - map of build tables with toolkit that was wrapped into dump.Entry
	Tables map[toolkit2.Oid]*dump.Table
	// Types - list of custom types that are used in DB schema
	Types []*toolkit2.Type
	// DataSectionObjects - list of objects to dump in data-section. There are sequences, tables and large objects
	DataSectionObjects []dump.Entry
	// Warnings - list of occurred ValidationWarning during validation and config building
	Warnings toolkit2.ValidationWarnings
	// Registry - registry of all the registered transformers definition
	Registry *transformersUtils.TransformerRegistry
	// TypeMap - map of registered types including custom types. It's common for the whole runtime
	TypeMap *pgtype.Map
}

// NewRuntimeContext - creating new runtime context.
// TODO: Recheck it is working properly. In a few cases (stages such as parameters building, schema validation) if
//
//	warnings are fatal procedure must be terminated immediately due to lack of objects required on the next step
func NewRuntimeContext(
	ctx context.Context, tx pgx.Tx, cfg []*domains.Table, r *transformersUtils.TransformerRegistry, opt *pgdump.Options,
	version int,
) (*RuntimeContext, error) {
	typeMap := tx.Conn().TypeMap()
	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("cannot discover types: %w", err)
	}
	if len(types) > 0 {
		toolkit2.TryRegisterCustomTypesV2(typeMap, types, true)
	}

	tables, warnings, err := validateAndBuildTablesConfig(ctx, tx, typeMap, cfg, r, version, types)
	if err != nil {
		return nil, fmt.Errorf("cannot validate and build table config: %w", err)
	}

	dataSectionObjects, err := getDumpObjects(ctx, tx, opt, tables)
	if err != nil {
		return nil, fmt.Errorf("cannot build dump object list: %w", err)
	}

	return &RuntimeContext{
		Tables:             tables,
		Types:              types,
		DataSectionObjects: dataSectionObjects,
		Warnings:           warnings,
		Registry:           r,
	}, nil
}

func (rc *RuntimeContext) IsFatal() bool {
	return rc.Warnings.IsFatal()
}
