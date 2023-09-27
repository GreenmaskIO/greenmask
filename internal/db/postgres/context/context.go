package context

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/internal/domains"
	toolkit "github.com/greenmaskio/greenmask/pkg/toolkit/transformers"
)

// RuntimeContext - describes current runtime behaviour according to the config and schema objects
type RuntimeContext struct {
	// Tables - map of build tables with toolkit that was wrapped into dump.Entry
	Tables map[toolkit.Oid]*dump.Table
	// Types - list of custom types that are used in DB schema
	Types []*toolkit.Type
	// DataSectionObjects - list of objects to dump in data-section. There are sequences, tables and large objects
	DataSectionObjects []dump.Entry
	// Warnings - list of occurred ValidationWarning during validation and config building
	Warnings toolkit.ValidationWarnings
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
		tryRegisterCustomTypesV2(typeMap, types)
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

func tryRegisterCustomTypesV2(typeMap *pgtype.Map, types []*toolkit.Type) {
	for _, t := range types {
		// Test is this type already registered
		_, ok := typeMap.TypeForOID(uint32(t.Oid))
		if ok {
			continue
		}
		if t.Kind == 'd' {
			if t.BaseType != 0 {
				baseType, ok := typeMap.TypeForOID(uint32(t.BaseType))
				if !ok {
					log.Warn().
						Str("Context", "CustomTypeRegistering").
						Str("Schema", t.Schema).
						Str("Name", t.Name).
						Int("Oid", int(t.Oid)).
						Str("Kind", fmt.Sprintf("%c", t.Kind)).
						Msg("unable to register domain type")
					continue
				}
				typeMap.RegisterType(&pgtype.Type{
					Name:  t.Name,
					OID:   uint32(t.Oid),
					Codec: baseType.Codec,
				})
				arrayType, ok := typeMap.TypeForName(fmt.Sprintf("_%s", baseType.Name))
				if !ok {
					log.Warn().
						Str("Context", "CustomTypeRegistering").
						Str("Schema", t.Schema).
						Str("Name", t.Name).
						Int("Oid", int(t.Oid)).
						Msg("cannot register array type for custom type")
					continue
				}
				arrayTypeName := fmt.Sprintf("_%s", t.Name)
				typeMap.RegisterType(&pgtype.Type{
					Name:  arrayTypeName,
					OID:   uint32(t.ArrayType),
					Codec: arrayType.Codec,
				})
			}
		} else {
			log.Debug().
				Str("Context", "CustomTypeRegistering").
				Str("Schema", t.Schema).
				Str("Name", t.Name).
				Int("Oid", int(t.Oid)).
				Str("Kind", fmt.Sprintf("%c", t.Kind)).
				Msg("Only domain types can be automatically registered: skipping")
		}
	}
}
