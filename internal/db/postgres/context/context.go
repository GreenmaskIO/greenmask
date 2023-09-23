package context

import (
	"context"
	"fmt"
	"github.com/greenmaskio/greenmask/internal/domains"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/db/postgres/dump"
	"github.com/greenmaskio/greenmask/internal/db/postgres/pgdump"
	transformersUtils "github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
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
func NewRuntimeContext(ctx context.Context, tx pgx.Tx, cfg []*domains.Table, r *transformersUtils.TransformerRegistry, opt *pgdump.Options, version int) (*RuntimeContext, error) {
	typeMap := tx.Conn().TypeMap()
	types, err := getCustomTypesUsedInTables(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("cannot discover types: %w", err)
	}
	if len(types) > 0 {
		tryRegisterCustomTypes(typeMap, types)
	}

	tables, warnings, err := validateAndBuildTablesConfig(ctx, tx, typeMap, cfg, r, version)
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

// TODO: Refactor this function
//  1. Rewrite CustomTypesUsedInTablesQuery - it might be recursive
//  2. Add Implement RangeType registering
func tryRegisterCustomTypes(typeMap *pgtype.Map, types []*toolkit.Type) {
	var unregisteredTypes = make([]*toolkit.Type, len(types))
	copiedCount := copy(unregisteredTypes, types)
	if copiedCount != len(types) {
		panic("copiedCount != types")
	}

	// TODO: Implement RangeType registering
	// Assuming that each custom type can be defined using another custom type as base, we can try to perform
	// nested loop. Every iteration of outer loop must be with registration at least one type. We perform
	// it until all types was registered or not anyone of resting types were registering during outer loop iteration.
	// We would build a dependencies tree, but currently it's N*N
	length := len(unregisteredTypes)
	var registeredOids = make([]toolkit.Oid, 0, len(unregisteredTypes))
	for i := 0; i < length; i++ {
		var registred bool
		for _, t := range unregisteredTypes {

			// Test is this type already registered
			_, ok := typeMap.TypeForOID(uint32(t.Oid))
			if ok {
				continue
			}

			// Try to register via BaseType:
			// 	1. Register base type
			// 	2. Register array type using naming _{{Type.Name}}
			if t.BaseType != 0 {
				baseType, ok := typeMap.TypeForOID(uint32(t.BaseType))
				// If not ok then it might be registered in the next outer iteration due to the nested base types
				// definition
				if ok {
					// Register base type
					typeMap.RegisterType(&pgtype.Type{
						Name:  t.Name,
						OID:   uint32(t.Oid),
						Codec: baseType.Codec,
					})
					registred = true
					registeredOids = append(registeredOids, t.Oid)

					// Register ArrayType
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

					// Trying to register ArrayType
					// Searching for arrayType
					expectedArrayTypeName := fmt.Sprintf("_%s", t.Name)
					idx := slices.IndexFunc(unregisteredTypes, func(t *toolkit.Type) bool {
						return t.Name == expectedArrayTypeName
					})
					if idx != -1 {
						customArrayType := unregisteredTypes[idx]
						typeMap.RegisterType(&pgtype.Type{
							Name:  customArrayType.Name,
							OID:   uint32(customArrayType.Oid),
							Codec: arrayType.Codec,
						})
						registeredOids = append(registeredOids, customArrayType.Oid)
					} else {
						log.Warn().
							Str("Context", "CustomTypeRegistering").
							Str("Schema", t.Schema).
							Str("Name", fmt.Sprintf("_%s", baseType.Name)).
							Int("Oid", -1).
							Msg("might be bug: cannot find custom array type codec: array type was not gathered")
					}

				}
			}
		}
		if registred {
			for _, oid := range registeredOids {
				idx := slices.IndexFunc(unregisteredTypes, func(t *toolkit.Type) bool {
					return t.Oid == oid
				})
				if idx == -1 {
					panic("unexpected registered type pos")
				}
				unregisteredTypes = slices.Delete(unregisteredTypes, idx, idx+1)
			}
			registeredOids = registeredOids[:0]
		} else {
			// If no one type was registered seems we cannot continue registering
			break
		}
	}
	if len(unregisteredTypes) > 0 {
		for _, t := range unregisteredTypes {
			log.Warn().
				Str("Context", "CustomTypeRegistering").
				Str("Schema", t.Schema).
				Str("Name", t.Name).
				Int("Oid", int(t.Oid)).
				Msg("cannot register custom type in the driver")
		}
	}
}
