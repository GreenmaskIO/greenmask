package context

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/config"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/domains/dump"
	"github.com/wwoytenko/greenfuscator/internal/db/postgres/pgdump"
	toolkit "github.com/wwoytenko/greenfuscator/internal/toolkit/transformers"
)

// RuntimeContext - describes current runtime behaviour according to the config and schema objects
type RuntimeContext struct {
	// Tables - map of build tables with transformers that was wrapped into dump.Entry
	Tables map[toolkit.Oid]*dump.Table
	// Types - list of custom types that are used in DB schema
	Types []*toolkit.Type
	// DataSectionObjects - list of objects to dump in data-section. There are sequences, tables and large objects
	DataSectionObjects []dump.Entry
	// Warning - list of occurred ValidationWarning during validation and config building
	Warning toolkit.ValidationWarnings
	// TransformerMap - map of available transformer definitions
	TransformerMap map[string]*toolkit.Definition
}

func NewRuntimeContext(ctx context.Context, tx pgx.Tx, cfg []*config.Table, tm map[string]*toolkit.Definition, opt *pgdump.Options) (*RuntimeContext, error) {
	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("cannot discover types: %w", err)
	}

	tables, warnings, err := validateAndBuildTablesConfig(ctx, tx, cfg, tm)
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
		Warning:            warnings,
		TransformerMap:     tm,
	}, nil
}

func (rc *RuntimeContext) IsFatal() bool {
	return rc.Warning.IsFatal()
}
