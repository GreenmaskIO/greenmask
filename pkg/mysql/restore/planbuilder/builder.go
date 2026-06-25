// Copyright 2025 Greenmask
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

// Package planbuilder converts persisted Metadata into a typed RestorePlan for
// MySQL. It is the single deserialization boundary: it reads
// RestorationItem.ObjectDefinition ([]byte) once and produces
// ObjectRestoreSpec.Payload (typed Go struct) that factories type-assert
// directly at restore time.
package planbuilder

import (
	"context"
	"encoding/json"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/factory/schema"
)

var _ core.RestorePlanBuilder = (*Builder)(nil)

// Builder implements core.RestorePlanBuilder for MySQL.
type Builder struct{}

func New() *Builder { return &Builder{} }

func (b *Builder) Build(
	_ context.Context,
	meta core.Metadata,
) (core.RestorePlan, error) {
	objectSpecs, err := b.buildObjectSpecs(meta)
	if err != nil {
		return core.RestorePlan{}, fmt.Errorf("build object specs: %w", err)
	}

	schemaSpecs, err := b.buildSchemaSpecs(meta)
	if err != nil {
		return core.RestorePlan{}, fmt.Errorf("build schema specs: %w", err)
	}

	var restorationCtx core.RestorationContext
	if meta.DataDump != nil {
		restorationCtx = meta.DataDump.DumpStat.RestorationContext
	}

	return core.RestorePlan{
		ObjectRestoreSpecs: objectSpecs,
		SchemaRestoreSpecs: schemaSpecs,
		RestorationContext: restorationCtx,
	}, nil
}

func (b *Builder) buildObjectSpecs(meta core.Metadata) ([]core.ObjectRestoreSpec, error) {
	if meta.DataDump == nil {
		return nil, nil
	}
	specs := make([]core.ObjectRestoreSpec, 0, len(meta.DataDump.DumpStat.RestorationItems))
	for _, item := range meta.DataDump.DumpStat.RestorationItems {
		switch item.ObjectKind {
		case kinds.ObjectKindTable, core.ObjectKindTable:
			var table core.Table
			if err := json.Unmarshal(item.ObjectDefinition, &table); err != nil {
				return nil, fmt.Errorf("unmarshal table (taskID=%d): %w", item.TaskID, err)
			}
			specs = append(specs, core.ObjectRestoreSpec{
				TaskID:      item.TaskID,
				Kind:        kinds.ObjectKindTable,
				Filename:    item.Filename,
				Compression: item.Compression,
				Format:      item.Format,
				RecordCount: item.RecordCount,
				Payload:     &table,
			})
		default:
			return nil, fmt.Errorf("unsupported object kind %q", item.ObjectKind)
		}
	}
	return specs, nil
}

func (b *Builder) buildSchemaSpecs(meta core.Metadata) ([]core.SchemaRestoreSpec, error) {
	if meta.SchemaDump == nil {
		return nil, nil
	}
	specs := make([]core.SchemaRestoreSpec, 0, len(meta.SchemaDump.DumpedDatabaseSchema))
	for _, stat := range meta.SchemaDump.DumpedDatabaseSchema {
		payload := schema.MysqlSchemaPayload{
			Stat:      stat,
			Databases: meta.Databases,
		}
		specs = append(specs, core.SchemaRestoreSpec{
			Kind:    kinds.SchemaObjectKindDatabase,
			Section: stat.Section,
			Payload: payload,
		})
	}
	return specs, nil
}
