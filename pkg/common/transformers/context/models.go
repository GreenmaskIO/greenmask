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

package context

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	commonparameters "github.com/greenmaskio/greenmask/pkg/common/transformers/parameters"
)

// TableDumpContext is a runtime transformation pipeline: it initialises,
// applies and tears down the table's transformers in order.
var _ core.Pipeliner = (*TableDumpContext)(nil)

// TransformerContext - supplied transformer and conditions that have to be executed.
type TransformerContext struct {
	Transformer core.Transformer
	// Condition - transformer level condition to evaluate before applying the transformer.
	Condition         core.CondEvaluator
	StaticParameters  map[string]*commonparameters.StaticParameter
	DynamicParameters map[string]*commonparameters.DynamicParameter
	// Source records where the transformation came from (explicit config vs
	// derivation). Zero value is treated as explicit; the derived context builder
	// sets it to derived.
	Source core.TransformationSource
}

func (tc *TransformerContext) SetRecordForDynamicParameters(r core.Recorder) {
	for _, param := range tc.DynamicParameters {
		param.SetRecord(r)
	}
}

func (tc *TransformerContext) EvaluateWhen(r core.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

func (tc *TransformerContext) Init(ctx context.Context) error {
	return tc.Transformer.Init(ctx)
}

func (tc *TransformerContext) Transform(ctx context.Context, r core.Recorder) error {
	return tc.Transformer.Transform(ctx, r)
}

func (tc *TransformerContext) Done(ctx context.Context) error {
	return tc.Transformer.Done(ctx)
}

func (tc *TransformerContext) GetAffectedColumns() map[int]string {
	return tc.Transformer.GetAffectedColumns()
}

func (tc *TransformerContext) Describe() string {
	return tc.Transformer.Describe()
}

// GetSnapshot builds a deterministic TransformationSnapshot from the initialized
// runtime parameters. Static parameters are read resolved (defaults applied) via
// RawValue; dynamic parameters are captured from their initialization settings
// (DynamicValue) since their per-record value is not available at snapshot time.
func (tc *TransformerContext) GetSnapshot(position int) (core.TransformationSnapshot, error) {
	static := make(map[string]any, len(tc.StaticParameters))
	for name, p := range tc.StaticParameters {
		raw, err := p.RawValue()
		if err != nil {
			return core.TransformationSnapshot{}, fmt.Errorf("read static parameter %q: %w", name, err)
		}
		static[name] = string(raw)
	}

	dynamic := make(map[string]any, len(tc.DynamicParameters))
	for name, p := range tc.DynamicParameters {
		if p.DynamicValue == nil {
			continue
		}
		// Store the resolved dynamic-parameter settings as-is; their per-record
		// value is not available at snapshot time.
		dynamic[name] = *p.DynamicValue
	}

	affected := affectedColumnNames(tc.GetAffectedColumns())

	ts := core.TransformationSnapshot{
		Name:                  tc.Describe(),
		Field:                 transformerField(tc.StaticParameters, affected),
		Position:              position,
		Source:                tc.snapshotSource(),
		StaticParameters:      mapOrNil(static),
		StaticParametersHash:  core.HashMap(static),
		DynamicParameters:     mapOrNil(dynamic),
		DynamicParametersHash: core.HashMap(dynamic),
		AffectedColumns:       affected,
		AffectedColumnsHash:   core.HashStrings(affected),
		Condition:             conditionExpression(tc.Condition),
	}
	ts.Fingerprint = core.TransformationFingerprint(ts)

	key, err := ts.StableKey()
	if err != nil {
		return core.TransformationSnapshot{}, fmt.Errorf("build transformation key: %w", err)
	}
	ts.Key = key
	return ts, nil
}

// snapshotSource returns the configured transformation source, defaulting to
// explicit when unset.
func (tc *TransformerContext) snapshotSource() core.TransformationSource {
	if tc.Source.Kind != "" {
		return tc.Source
	}
	return core.TransformationSource{Kind: core.TransformationSourceKindExplicit}
}

// TableDumpContext - everything related to the table that must be applied for a record.
// It contains table, transformers, dump query, table driver and conditions.
type TableDumpContext struct {
	Table              *core.Table
	TransformerContext []core.TransformerContexter
	// Condition - table level condition to evaluate before applying any transformers.
	Condition   core.CondEvaluator
	Query       string
	TableDriver core.TableDriver
	// ColumnKind is the engine-specific kind used for column attribute
	// identities in the snapshot (e.g. core.EntityKindMysqlColumn). Set by the
	// engine's context builder.
	ColumnKind core.EntityKind
	// Compression controls the output compression applied to the dumped table
	// data file (CompressionPgzip selects the parallel implementation). Set by
	// the engine's context builder from the run config, mirroring the schema
	// dump path. An unset value means CompressionNone.
	Compression core.Compression
}

func (tc *TableDumpContext) HasTransformer() bool {
	return len(tc.TransformerContext) > 0
}

func (tc *TableDumpContext) GetAffectedColumns() []int {
	affectedColumns := make(map[int]struct{})
	for _, transformerCtx := range tc.TransformerContext {
		ac := transformerCtx.GetAffectedColumns()
		for idx := range ac {
			affectedColumns[idx] = struct{}{}
		}
	}
	res := make([]int, 0, len(affectedColumns))
	for col := range affectedColumns {
		res = append(res, col)
	}
	return res
}
func (tc *TableDumpContext) EvaluateWhen(r core.Recorder) (bool, error) {
	if tc.Condition == nil {
		return true, nil
	}
	return tc.Condition.Evaluate(r)
}

func (tc *TableDumpContext) Init(ctx context.Context) error {
	for i, transformerCtx := range tc.TransformerContext {
		if err := transformerCtx.Init(ctx); err != nil {
			return fmt.Errorf("initialize transformer pos=%d name='%s': %w",
				i, transformerCtx.Describe(), err,
			)
		}
	}
	return nil
}

// Transform applies every configured transformer to the record in order. The
// table-level condition is evaluated first; when it is false the record passes
// through untouched. Each transformer's own condition is then evaluated before
// it runs. This makes TableDumpContext a core.Pipeliner.
func (tc *TableDumpContext) Transform(ctx context.Context, r core.Recorder) error {
	needTransform, err := tc.EvaluateWhen(r)
	if err != nil {
		return fmt.Errorf("evaluate table condition: %w", err)
	}
	if !needTransform {
		return nil
	}
	for i, transformerCtx := range tc.TransformerContext {
		transformerCtx.SetRecordForDynamicParameters(r)
		ok, err := transformerCtx.EvaluateWhen(r)
		if err != nil {
			return fmt.Errorf("evaluate transformer pos=%d name='%s' condition: %w",
				i, transformerCtx.Describe(), err)
		}
		if !ok {
			continue
		}
		if err := transformerCtx.Transform(ctx, r); err != nil {
			return fmt.Errorf("transform record using transformer pos=%d name='%s': %w",
				i, transformerCtx.Describe(), err)
		}
	}
	return nil
}

// Done terminates every transformer, accumulating errors so a single failure
// does not prevent the rest from releasing their resources.
func (tc *TableDumpContext) Done(ctx context.Context) error {
	var lastErr error
	for i, transformerCtx := range tc.TransformerContext {
		if err := transformerCtx.Done(ctx); err != nil {
			lastErr = errors.Join(lastErr,
				fmt.Errorf("terminate transformer pos=%d name='%s': %w",
					i, transformerCtx.Describe(), err))
		}
	}
	if lastErr != nil {
		return fmt.Errorf("terminate transformer: %w", lastErr)
	}
	return nil
}

// GetSnapshot builds the engine-agnostic portion of an ObjectSnapshot from the
// runtime context: column attributes, subset query, table-level condition and
// per-transformer snapshots. Identity/key/need-schema-dump are overlaid by the
// engine-specific snapshot builder.
func (tc *TableDumpContext) GetSnapshot() (core.ObjectSnapshot, error) {
	return BuildObjectSnapshot(tc.Table, tc.Query, tc.Condition, tc.TransformerContext, tc.ColumnKind)
}

// BuildObjectSnapshot assembles the engine-agnostic ObjectSnapshot fields shared
// by every TableContexter implementation. columnKind is the engine-specific kind
// used for column attribute identities.
func BuildObjectSnapshot(
	table *core.Table,
	query string,
	condition core.CondEvaluator,
	transformers []core.TransformerContexter,
	columnKind core.EntityKind,
) (core.ObjectSnapshot, error) {
	if table == nil {
		return core.ObjectSnapshot{}, fmt.Errorf("table is nil")
	}

	attributes, attributesHash, err := buildAttributes(table.Columns, columnKind)
	if err != nil {
		return core.ObjectSnapshot{}, fmt.Errorf("build attributes: %w", err)
	}

	snapshot := core.ObjectSnapshot{
		SubsetQuery:     query,
		SubsetQueryHash: core.HashString(query),
		Attributes:      attributes,
		AttributesHash:  attributesHash,
		Condition:       conditionSnapshot(condition),
	}

	if len(transformers) > 0 {
		transformations := make(map[core.StableKey]core.TransformationSnapshot, len(transformers))
		for i, t := range transformers {
			ts, err := t.GetSnapshot(i)
			if err != nil {
				return core.ObjectSnapshot{}, fmt.Errorf("build transformation snapshot pos=%d: %w", i, err)
			}
			if _, exists := transformations[ts.Key]; exists {
				return core.ObjectSnapshot{}, fmt.Errorf("duplicate transformation key %q", ts.Key)
			}
			transformations[ts.Key] = ts
		}
		snapshot.Transformations = transformations
	}

	return snapshot, nil
}

// buildAttributes projects table columns into the attribute map keyed by the
// attribute identity's stable key, plus a deterministic hash computed over the
// columns in declared order (position included).
func buildAttributes(columns []core.Column, columnKind core.EntityKind) (map[core.StableKey]core.ObjectAttribute, string, error) {
	if len(columns) == 0 {
		return nil, "", nil
	}
	attributes := make(map[core.StableKey]core.ObjectAttribute, len(columns))
	signatures := make([]string, 0, len(columns))
	for _, col := range columns {
		identity := core.ColumnAttributeIdentity(columnKind, col.Name)
		key, err := identity.StableKey()
		if err != nil {
			return nil, "", fmt.Errorf("build attribute key for column %q: %w", col.Name, err)
		}
		attributes[key] = core.ObjectAttribute{
			Identity:   identity,
			Position:   col.Idx,
			Definition: core.AttributeDefinition(col.TypeName),
		}
		signatures = append(signatures, fmt.Sprintf("%d:%s=%s", col.Idx, col.Name, col.TypeName))
	}
	return attributes, core.HashStrings(signatures), nil
}

// conditionExpression returns the normalized condition expression from a
// CondEvaluator, or "" when there is none.
func conditionExpression(condition core.CondEvaluator) string {
	if condition == nil {
		return ""
	}
	return condition.Expression()
}

// conditionSnapshot builds the object-level condition snapshot from a
// CondEvaluator (returns nil when there is no condition).
func conditionSnapshot(condition core.CondEvaluator) *core.TransformationConditionSnapshot {
	return core.NewConditionSnapshot(conditionExpression(condition))
}

// affectedColumnNames returns the affected column names ordered by column index.
func affectedColumnNames(affected map[int]string) []string {
	if len(affected) == 0 {
		return nil
	}
	indexes := make([]int, 0, len(affected))
	for idx := range affected {
		indexes = append(indexes, idx)
	}
	slices.Sort(indexes)
	names := make([]string, 0, len(affected))
	for _, idx := range indexes {
		names = append(names, affected[idx])
	}
	return names
}

// affectedColumnsDelimiter joins the columns a transformation affects into a
// single field reference value (a transformation may affect more than one
// column). The columns are pre-sorted by index, so the result is deterministic.
const affectedColumnsDelimiter = ","

// transformerField picks the field a transformation is keyed on: all affected
// columns (delimiter-joined) when known, otherwise the "column" static parameter,
// otherwise an expression field.
func transformerField(
	static map[string]*commonparameters.StaticParameter,
	affected []string,
) core.ObjectFieldRef {
	if len(affected) > 0 {
		return core.ObjectFieldRef{
			Kind:  core.FieldRefKindColumn,
			Value: strings.Join(affected, affectedColumnsDelimiter),
		}
	}
	if p, ok := static["column"]; ok {
		if raw, err := p.RawValue(); err == nil && len(raw) > 0 {
			return core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: string(raw)}
		}
	}
	return core.ObjectFieldRef{Kind: core.FieldRefKindExpression}
}

// mapOrNil returns nil for an empty map so omitempty JSON fields stay absent.
func mapOrNil(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	return m
}
