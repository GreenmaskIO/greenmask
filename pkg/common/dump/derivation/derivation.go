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

// Package derivation implements the engine-agnostic transformation inheritance
// algorithm (apply_for_references): when a deterministic transformer is
// configured on a primary/unique-key column with apply_for_references, it is
// propagated onto every foreign-key column that references it, recursively along
// end-to-end identifier chains, so both sides of the FK receive the same
// transformed value and referential integrity survives anonymization.
//
// The algorithm mirrors the v1 PostgreSQL implementation
// (buildRefsWithEndToEndDfs / processReference) but consumes only engine-agnostic
// core types plus the shared transformercontext payloads, so any engine adapter
// can reuse it. Engine-specific bits (table driver construction, transformer
// registry) are injected by the caller.
package derivation

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// Condition namespaces used to rewrite inherited when-expressions from the parent
// column to the referencing foreign-key column. They mirror the expr evaluator's
// record/raw_record namespaces (pkg/common/conditions/expr.go); they are
// duplicated here as literals to avoid importing the conditions package for two
// constants.
const (
	recordNamespace    = "record"
	rawRecordNamespace = "raw_record"
)

// TableDriverFactory builds a table driver for a table that does not yet have
// one (raw children gain a derived transformer and therefore need a driver to
// initialise it). It is the single engine-specific dependency of the algorithm.
type TableDriverFactory interface {
	NewTableDriver(ctx context.Context, table core.Table, columnsTypeOverride map[string]string) (core.TableDriver, error)
}

// Deriver runs the reference-propagation algorithm.
type Deriver struct {
	registry      core.TransformerRegistry
	driverFactory TableDriverFactory
}

// New builds a Deriver wired to the transformer registry (used to instantiate
// derived transformers) and the engine's table driver factory.
func New(registry core.TransformerRegistry, driverFactory TableDriverFactory) *Deriver {
	return &Deriver{registry: registry, driverFactory: driverFactory}
}

// rootMapping is an eligible root transformer on a primary-key column that must
// be propagated onto referencing tables.
//
// "root" here means the ORIGIN of the whole reference chain, not the immediate
// foreign-key parent of any given descendant. In a chain a -> b -> c the derived
// transformers on both b and c carry the rootIdentity/rootColumn of a: every hop
// clones a's configuration, so the provenance always points back to where the
// value originates, not to the intermediate table that happens to be one FK away.
type rootMapping struct {
	// index uniquely identifies this root within its root table; it keys the
	// per-edge cycle guard so the same root is applied at most once per edge.
	index int
	// rootColumn is the primary-key column of the chain's root table that the
	// transformer targets. It stays fixed across every hop of the chain.
	rootColumn string
	// transformerName is the transformer's registry name.
	transformerName string
	// config is a clonable copy of the root transformer configuration.
	config *core.TransformerConfig
	// columnTypeOverride is the root's columns_type_override for rootColumn,
	// carried onto the referencing column ("" when none).
	columnTypeOverride string
	// rootIdentity is the stable identity of the chain's root object, recorded on
	// the derived transformation source (DerivedFrom.Object). For a multi-hop
	// chain this is the root, NOT the immediate foreign-key parent.
	rootIdentity core.EntityIdentity
}

// visitedKey guards against cycles / over-processing: a (edge, root) pair is
// processed at most once.
type visitedKey struct {
	edge    string
	rootIdx int
}

// Derive returns a DumpContext equal to in.ExplicitCtx with derived transformers
// merged into referencing tables. It emits fatal validation warnings (and returns
// core.ErrFatalValidationError) when a transformer flagged apply_for_references is
// not eligible for propagation.
func (d *Deriver) Derive(ctx context.Context, in core.DerivedDumpContextInput) (core.DumpContext, error) {
	// Work on a copy of the spec slice so the explicit context is left intact;
	// SchemaDumpSpecs and Source pass through unchanged.
	result := core.DumpContext{
		DumpObjectSpecs: slices.Clone(in.ExplicitCtx.DumpObjectSpecs),
		SchemaDumpSpecs: in.ExplicitCtx.SchemaDumpSpecs,
		Source:          in.ExplicitCtx.Source,
	}

	// Index specs by ObjectID so the traversal addresses tables by their identity
	// rather than a slice position. The pointers reference the (already cloned)
	// backing array, so mutating a spec through the map updates result directly.
	specByID := make(map[core.ObjectID]*core.ObjectDumpSpec, len(result.DumpObjectSpecs))
	for i := range result.DumpObjectSpecs {
		spec := &result.DumpObjectSpecs[i]
		specByID[spec.ObjectID] = spec
	}

	// Build the reverse foreign-key adjacency (parent -> incoming edges). Object
	// graph edges point child(From) -> parent(To); to walk from a referenced
	// parent to its referencing children we index edges by their To endpoint.
	incoming := make(map[core.ObjectID][]core.ObjectEdge)
	for _, edges := range in.DependencyGraphResult.ObjectGraph.Edges {
		for _, e := range edges {
			incoming[e.To] = append(incoming[e.To], e)
		}
	}
	// Order each parent's incoming edges by a stable key. incoming is assembled by
	// ranging over a Go map, whose iteration order is randomized per process. Left
	// unordered, the sequence in which derived transformers are appended to a table
	// would differ run-to-run; because a transformation's snapshot key embeds its
	// positional index (see core.TransformationSnapshot.StableKey), two dumps of an
	// identical database/config would then report spurious drift. A deterministic
	// traversal keeps derived positions — and therefore the snapshot — stable.
	for id := range incoming {
		slices.SortFunc(incoming[id], func(a, b core.ObjectEdge) int {
			return strings.Compare(edgeKey(a), edgeKey(b))
		})
	}

	hasFatal := false
	// Iterate the specs deterministically by their slice order.
	for i := range result.DumpObjectSpecs {
		spec := result.DumpObjectSpecs[i]
		payload, ok := spec.Payload.(transformercontext.TableDumpContext)
		if !ok || payload.Table == nil {
			log.Ctx(ctx).Debug().
				Int("ObjectID", int(spec.ObjectID)).
				Str("ObjectName", spec.Name).
				Str("PayloadType", fmt.Sprintf("%T", spec.Payload)).
				Msg("derivation: skipping spec, not a table dump context")
			continue
		}

		// Gate the whole table: if any apply_for_references transformer fails the
		// eligibility requirements, emit a fatal warning and skip this table's
		// propagation entirely (mirrors v1 checkApplyForReferenceMetRequirements).
		if fatal := d.checkRequirements(ctx, payload); fatal {
			hasFatal = true
			continue
		}

		roots := d.collectRoots(ctx, payload, spec.Identity, in.TableConfigs)
		if len(roots) == 0 {
			continue
		}

		active := make(map[string][]*rootMapping)
		for _, rt := range roots {
			active[rt.rootColumn] = append(active[rt.rootColumn], rt)
		}
		visited := make(map[visitedKey]bool)
		// A derivation failure is fail-fast: return immediately rather than
		// descending further or mutating more specs. The eligibility gate above
		// (checkRequirements) still collects all its warnings across tables.
		if err := d.dfs(ctx, spec.ObjectID, active, false, incoming, specByID, visited); err != nil {
			return result, err
		}
	}

	if hasFatal {
		return result, core.ErrFatalValidationError
	}

	logDerivationSummary(ctx, result.DumpObjectSpecs)
	return result, nil
}

// logDerivationSummary emits a single informational line describing how many
// derived transformers were produced and on how many tables, so a successful
// run leaves a trace of what the derivation actually did (nothing is logged when
// no derivation happened).
func logDerivationSummary(ctx context.Context, specs []core.ObjectDumpSpec) {
	derived := 0
	tables := 0
	for _, spec := range specs {
		payload, ok := spec.Payload.(transformercontext.TableDumpContext)
		if !ok {
			continue
		}
		onThisTable := 0
		for _, tc := range payload.TransformerContext {
			tctx, ok := tc.(*transformercontext.TransformerContext)
			if ok && tctx.Source.Kind == core.TransformationSourceKindDerived {
				onThisTable++
			}
		}
		if onThisTable > 0 {
			derived += onThisTable
			tables++
		}
	}
	if derived == 0 {
		return
	}
	log.Ctx(ctx).Info().
		Int("DerivedTransformers", derived).
		Int("Tables", tables).
		Msg("derivation: propagated deterministic transformers onto referencing columns")
}

// checkRequirements reports whether the table has an apply_for_references
// transformer that is not eligible for propagation, emitting fatal warnings for
// each offender.
func (d *Deriver) checkRequirements(ctx context.Context, payload transformercontext.TableDumpContext) bool {
	fatal := false
	for _, tc := range payload.TransformerContext {
		tctx, ok := tc.(*transformercontext.TransformerContext)
		if !ok || !tctx.ApplyForReferences {
			continue
		}
		// A derived transformer is itself a product of propagation (its cloned
		// config keeps ApplyForReferences), not a user opt-in — skip it so a table
		// that was mutated earlier in this pass is not re-validated as a root.
		if tctx.Source.Kind == core.TransformationSourceKindDerived {
			continue
		}
		if !tctx.AllowApplyForReferenced {
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				AddMeta(core.MetaKeyTransformerName, tctx.Describe()).
				SetMsg("cannot apply transformer for references: transformer does not support apply_for_references"))
			fatal = true
			continue
		}
		// Reference propagation retargets a single column (it rewrites the "column"
		// static parameter and maps it through the foreign key). A transformer that
		// affects several columns cannot be retargeted this way, so reject it rather
		// than silently propagating only its first primary-key column.
		if len(tctx.GetAffectedColumns()) > 1 {
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				AddMeta(core.MetaKeyTransformerName, tctx.Describe()).
				SetMsg("cannot apply transformer for references: transformer affects multiple columns, " +
					"which is not supported for apply_for_references"))
			fatal = true
			continue
		}
		if !tctx.IsDeterministic() {
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				AddMeta(core.MetaKeyTransformerName, tctx.Describe()).
				SetMsg("cannot apply transformer for references: engine is not deterministic " +
					"(set engine: hash/deterministic so the same value is produced on both sides of the foreign key)"))
			fatal = true
		}
	}
	return fatal
}

// collectRoots gathers the eligible root transformers on the root table whose
// affected column is part of the table primary key. rootIdentity is the identity
// of that root table, recorded verbatim on every derived transformation down the
// chain (see rootMapping).
func (d *Deriver) collectRoots(
	ctx context.Context,
	payload transformercontext.TableDumpContext,
	rootIdentity core.EntityIdentity,
	tableConfigs []core.TableConfig,
) []*rootMapping {
	var roots []*rootMapping
	idx := 0
	for _, tc := range payload.TransformerContext {
		tctx, ok := tc.(*transformercontext.TransformerContext)
		if !ok {
			continue
		}
		if !tctx.ApplyForReferences || !tctx.AllowApplyForReferenced || !tctx.IsDeterministic() {
			continue
		}
		// Skip transformers this pass already derived: the full end-to-end chain is
		// walked from the real root by dfs, so a derived transformer must never be
		// re-collected as a root (that would re-propagate and, since it has no
		// originating config, emit a spurious "config not found" warning). Only the
		// root's own DFS is authoritative.
		if tctx.Source.Kind == core.TransformationSourceKindDerived {
			continue
		}
		rootCol := affectedPrimaryKeyColumn(tctx.GetAffectedColumns(), payload.Table.PrimaryKey)
		if rootCol == "" {
			// The user opted a transformer into apply_for_references, but its column
			// is not part of the primary key, so nothing references it as an
			// identifier and the opt-in silently does nothing. Surface it as a
			// (non-fatal) warning rather than dropping it without a trace.
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityWarning).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				AddMeta(core.MetaKeyTransformerName, tctx.Describe()).
				SetMsg("apply_for_references has no effect: the transformer's column is not part of " +
					"the primary key, so no foreign key references it"))
			continue
		}
		cfg := findRootConfig(tableConfigs, payload.Table.Schema, payload.Table.Name, tctx.Describe(), rootCol)
		if cfg == nil {
			// An eligible transformer whose originating config could not be located
			// (name/column mismatch) cannot be cloned onto references. This is
			// unexpected — the explicit builder built this context from a config —
			// so warn rather than silently skipping propagation.
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityWarning).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				AddMeta(core.MetaKeyTransformerName, tctx.Describe()).
				AddMeta(core.MetaKeyColumnName, rootCol).
				SetMsg("cannot propagate transformer for references: its source configuration " +
					"was not found, so it will not be applied to referencing columns"))
			continue
		}
		roots = append(roots, &rootMapping{
			index:              idx,
			rootColumn:         rootCol,
			transformerName:    tctx.Describe(),
			config:             cfg,
			columnTypeOverride: columnTypeOverride(tableConfigs, payload.Table.Schema, payload.Table.Name, rootCol),
			rootIdentity:       rootIdentity,
		})
		idx++
	}
	return roots
}

// dfs walks referencing children of parentID, applying active root transformers
// onto their foreign-key columns and descending along end-to-end identifier
// chains. It returns a non-nil error and stops immediately (fail-fast) the moment
// a derivation emits a fatal validation warning, so it never descends past a
// scope that failed to initialise.
//
// Parameters:
//   - ctx: carries the logger and the validation collector; warnings raised while
//     instantiating derived transformers are added to it.
//   - parentID: the referenced (parent) object whose incoming foreign keys are
//     followed to its referencing children on this level.
//   - active: the transformers still propagating, keyed by the column they target
//     on the current level (a parent primary-key column at the top level, a
//     child's own key column deeper down). One column may carry several roots
//     (composite keys), hence the slice.
//   - checkEndToEnd: false at the first level, where every direct foreign-key
//     child inherits; true deeper, where a child is processed only when its
//     foreign-key column is also part of its own primary key (an end-to-end
//     identifier).
//   - incoming: the reverse foreign-key adjacency, mapping a parent ObjectID to
//     the edges of its referencing children (edges point child->parent, indexed
//     here by their To endpoint).
//   - specByID: every dump spec addressed by ObjectID; the pointers alias
//     result.DumpObjectSpecs so a derived transformer is merged in place.
//   - visited: cycle / over-processing guard keyed by (edge, root index); a given
//     root is applied to a given edge at most once, which terminates foreign-key
//     cycles.
func (d *Deriver) dfs(
	ctx context.Context,
	parentID core.ObjectID,
	active map[string][]*rootMapping,
	checkEndToEnd bool,
	incoming map[core.ObjectID][]core.ObjectEdge,
	specByID map[core.ObjectID]*core.ObjectDumpSpec,
	visited map[visitedKey]bool,
) error {
	if len(active) == 0 {
		return nil
	}
	// Iterate active mappings in a stable column order so derived transformers are
	// appended deterministically (see the incoming-edge ordering in Derive for the
	// snapshot-stability rationale).
	activeCols := sortedActiveColumns(active)
	for _, e := range incoming[parentID] {
		childSpec := specByID[e.From]
		if childSpec == nil {
			// The dependency graph references a child object that has no dump spec
			// (e.g. filtered out of the dump). Propagation cannot reach it.
			log.Ctx(ctx).Debug().
				Int("ParentID", int(parentID)).
				Int("ChildID", int(e.From)).
				Msg("derivation: skipping edge, referencing child has no dump spec")
			continue
		}
		fk, ok := e.Link.Payload.(core.ForeignKeyLinkPayload)
		if !ok {
			log.Ctx(ctx).Debug().
				Int("ParentID", int(parentID)).
				Int("ChildID", int(e.From)).
				Str("PayloadType", fmt.Sprintf("%T", e.Link.Payload)).
				Msg("derivation: skipping edge, link payload is not a foreign key")
			continue
		}
		childPayload, ok := childSpec.Payload.(transformercontext.TableDumpContext)
		if !ok || childPayload.Table == nil {
			log.Ctx(ctx).Debug().
				Int("ChildID", int(e.From)).
				Str("ChildName", childSpec.Name).
				Str("PayloadType", fmt.Sprintf("%T", childSpec.Payload)).
				Msg("derivation: skipping edge, child spec is not a table dump context")
			continue
		}
		childPK := sliceToSet(childPayload.Table.PrimaryKey)

		// Compute the mappings that continue deeper: a foreign-key column that is
		// also part of the child primary key is an end-to-end identifier.
		childActive := make(map[string][]*rootMapping)
		for _, parentCol := range activeCols {
			childCol := mapColumn(fk, parentCol)
			if childCol == "" {
				continue
			}
			if _, isPK := childPK[childCol]; isPK {
				childActive[childCol] = append(childActive[childCol], active[parentCol]...)
			}
		}

		if checkEndToEnd && len(childActive) == 0 {
			continue
		}

		edgeID := edgeKey(e)
		didWork := false
		for _, parentCol := range activeCols {
			childCol := mapColumn(fk, parentCol)
			if childCol == "" {
				continue
			}
			for _, rt := range active[parentCol] {
				vk := visitedKey{edge: edgeID, rootIdx: rt.index}
				if visited[vk] {
					continue
				}
				visited[vk] = true
				didWork = true
				// Fail fast: a failed derivation leaves this child only partially
				// initialised. Descending into the end-to-end chain below it would
				// propagate from a broken scope, so abort the whole walk — the build
				// is doomed to fail with a fatal validation error regardless.
				if err := d.applyDerived(ctx, childSpec, childCol, rt); err != nil {
					return err
				}
			}
		}

		// Descend only along end-to-end chains, and only when this edge produced
		// new work — otherwise a foreign-key cycle would recurse forever.
		if len(childActive) == 0 || !didWork {
			continue
		}
		if err := d.dfs(ctx, e.From, childActive, true, incoming, specByID, visited); err != nil {
			return err
		}
	}
	return nil
}

// applyDerived instantiates the derived transformer for (child table, childCol)
// and merges it into the child spec, flipping a previously-raw child to
// transformed/derived origin. It returns core.ErrFatalValidationError (after
// recording the corresponding validation warning) when the derivation cannot be
// completed, so the caller aborts the build.
func (d *Deriver) applyDerived(
	ctx context.Context,
	spec *core.ObjectDumpSpec,
	childCol string,
	rt *rootMapping,
) error {
	payload, ok := spec.Payload.(transformercontext.TableDumpContext)
	if !ok || payload.Table == nil {
		log.Ctx(ctx).Debug().
			Int("ObjectID", int(spec.ObjectID)).
			Str("ObjectName", spec.Name).
			Str("PayloadType", fmt.Sprintf("%T", spec.Payload)).
			Str("ChildColumn", childCol).
			Str(core.MetaKeyTransformerName, rt.transformerName).
			Msg("derivation: skipping derived transformer, child spec is not a table dump context")
		return nil
	}

	// User config wins: if the child already has ANY explicit transformer on this
	// column, the user has taken control of it, so reference propagation must not
	// touch it — regardless of whether the explicit transformer has the same name
	// as the inherited one. (v1 only deferred to a same-name transformer, which let
	// a different explicit transformer be stacked on top and silently break the FK.)
	if explicitName := explicitTransformerOnColumn(payload.TransformerContext, childCol); explicitName != "" {
		if explicitName == rt.transformerName {
			// Same transformer the user likely configured to mirror the parent — fine.
			log.Ctx(ctx).Info().
				Str(core.MetaKeyTransformerName, rt.transformerName).
				Str("RootColumn", rt.rootColumn).
				Str(core.MetaKeyTableSchema, payload.Table.Schema).
				Str(core.MetaKeyTableName, payload.Table.Name).
				Str("ChildColumn", childCol).
				Msg("skipping apply transformer for reference: column already has a matching manually configured transformer")
			return nil
		}
		// A different explicit transformer very likely does not reproduce the
		// referenced key, so the foreign key may break. Skip propagation (the user
		// owns the column) but surface it rather than hiding a probable integrity loss.
		validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityWarning).
			AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
			AddMeta(core.MetaKeyTableName, payload.Table.Name).
			AddMeta(core.MetaKeyColumnName, childCol).
			AddMeta(core.MetaKeyTransformerName, explicitName).
			SetMsg(fmt.Sprintf("foreign-key column has its own %q transformer differing from the inherited %q; "+
				"reference propagation skipped and referential integrity may not be preserved",
				explicitName, rt.transformerName)))
		return nil
	}

	// A column reached through more than one foreign-key path must not receive a
	// derived transformer twice.
	if existing := existingDerivedOnColumn(payload.TransformerContext, childCol); existing != nil {
		if sameRoot(existing, rt) {
			// The same root reached this column via another foreign-key path (a
			// diamond, e.g. tenant_id flowing through two composite keys into one
			// child). One application already produces the value every path expects;
			// applying it again would transform an already-transformed value and
			// break integrity on every path.
			log.Ctx(ctx).Debug().
				Str(core.MetaKeyTransformerName, rt.transformerName).
				Str(core.MetaKeyTableSchema, payload.Table.Schema).
				Str(core.MetaKeyTableName, payload.Table.Name).
				Str("ChildColumn", childCol).
				Msg("derivation: skipping duplicate derived transformer reached via another foreign-key path")
			return nil
		}
		// Two different roots anonymize this column's referenced keys differently,
		// so no single value can satisfy both foreign keys — referential integrity
		// is unavoidably lost for one of them. Keep the first derivation and warn
		// rather than stacking a second transformer that would break both.
		validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityWarning).
			AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
			AddMeta(core.MetaKeyTableName, payload.Table.Name).
			AddMeta(core.MetaKeyColumnName, childCol).
			AddMeta(core.MetaKeyTransformerName, rt.transformerName).
			SetMsg("cannot apply transformer for references: column is referenced by two " +
				"differently-anonymized keys, so referential integrity cannot be preserved for both"))
		return nil
	}

	cfg := rt.config.Clone()
	if cfg.StaticParams == nil {
		cfg.StaticParams = make(map[string]core.ParamsValue)
	}
	cfg.StaticParams["column"] = core.ParamsValue(childCol)
	if cfg.When != "" {
		cfg.When = rewriteWhen(cfg.When, rt.rootColumn, childCol)
	}

	override := map[string]string(nil)
	if rt.columnTypeOverride != "" {
		override = map[string]string{childCol: rt.columnTypeOverride}
	}

	// Reuse the child's existing driver when it already has one (the table already
	// carries transformers); otherwise build one for this raw child.
	driver := payload.TableDriver
	if driver == nil {
		var err error
		driver, err = d.driverFactory.NewTableDriver(ctx, *payload.Table, override)
		if err != nil {
			validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
				SetSeverity(core.ValidationSeverityError).
				AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
				AddMeta(core.MetaKeyTableName, payload.Table.Name).
				SetError(err).
				SetMsg("cannot build table driver for derived transformer"))
			return core.ErrFatalValidationError
		}
	}

	provisioner, ok := d.registry.Get(rt.transformerName)
	if !ok {
		validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityError).
			AddMeta(core.MetaKeyTransformerName, rt.transformerName).
			SetMsg("derived transformer is not found in registry"))
		return core.ErrFatalValidationError
	}
	built, err := provisioner.Init(ctx, driver, *cfg)
	if err != nil {
		validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityError).
			AddMeta(core.MetaKeyTransformerName, rt.transformerName).
			AddMeta(core.MetaKeyTableSchema, payload.Table.Schema).
			AddMeta(core.MetaKeyTableName, payload.Table.Name).
			SetError(err).
			SetMsg("cannot initialise derived transformer"))
		return core.ErrFatalValidationError
	}
	derived, ok := built.(*transformercontext.TransformerContext)
	if !ok {
		validationcollector.FromContext(ctx).Add(core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityError).
			AddMeta(core.MetaKeyTransformerName, rt.transformerName).
			SetMsg(fmt.Sprintf("derived transformer has unexpected context type %T", built)))
		return core.ErrFatalValidationError
	}
	// DerivedFrom points at the ROOT of the reference chain, not the immediate
	// foreign-key parent. In a chain a -> b -> c, the transformer on c records
	// a's identity/column, because c's transformation is a clone of a's config
	// propagated through b — the root is the true origin of the value. The Reason
	// spells this out so a reader of the snapshot is not misled into thinking c
	// directly references the root table. (To instead record the immediate FK
	// parent, thread the current parent identity + FK constraint through the DFS.)
	rootName, nameErr := rt.rootIdentity.Name()
	if nameErr != nil {
		// A malformed root identity should never reach here (it was built by the
		// explicit context builder); fall back to the kind so the human-readable
		// reason stays populated rather than aborting the derivation.
		rootName = string(rt.rootIdentity.Kind)
		log.Ctx(ctx).Debug().Err(nameErr).
			Str(core.MetaKeyTransformerName, rt.transformerName).
			Msg("derivation: root identity has no resolvable name")
	}
	derived.Source = core.TransformationSource{
		Kind: core.TransformationSourceKindDerived,
		DerivedFrom: &core.TransformationDerivationRef{
			Object:   rt.rootIdentity,
			Field:    core.ObjectFieldRef{Kind: core.FieldRefKindColumn, Value: rt.rootColumn},
			LinkKind: core.ObjectLinkKindForeignKey,
		},
		Reason: fmt.Sprintf(
			"inherited from the root of the reference chain: primary-key column %q of %q",
			rt.rootColumn, rootName,
		),
	}

	wasRaw := spec.Mode == core.DumpModeRaw && len(payload.TransformerContext) == 0

	// Clone before append so the explicit context's backing array is never mutated.
	payload.TransformerContext = append(slices.Clone(payload.TransformerContext), derived)
	payload.TableDriver = driver

	// spec points into result.DumpObjectSpecs, so these writes update it in place.
	spec.Payload = payload
	spec.Mode = core.DumpModeTransformed
	if wasRaw {
		spec.Origin = core.ObjectOrigin{
			Kind: core.ObjectOriginDerived,
			Reason: fmt.Sprintf(
				"introduced by transformation inheritance from the reference-chain root %q (column %q)",
				rootName, rt.rootColumn,
			),
		}
	}
	return nil
}

// affectedPrimaryKeyColumn returns the affected column that is part of the
// primary key, or "" when the transformer affects no primary-key column.
func affectedPrimaryKeyColumn(affected map[int]string, primaryKey []string) string {
	pk := sliceToSet(primaryKey)
	// Iterate affected columns by index for deterministic selection.
	idxs := make([]int, 0, len(affected))
	for idx := range affected {
		idxs = append(idxs, idx)
	}
	slices.Sort(idxs)
	for _, idx := range idxs {
		name := affected[idx]
		if _, ok := pk[name]; ok {
			return name
		}
	}
	return ""
}

// mapColumn resolves the referencing foreign-key column that corresponds to the
// given referenced (parent) column, using the positional alignment
// RefColumns[i] <-> Columns[i]. Returns "" when the parent column is not part of
// this foreign key.
func mapColumn(fk core.ForeignKeyLinkPayload, parentColumn string) string {
	i := slices.Index(fk.RefColumns, parentColumn)
	if i < 0 || i >= len(fk.Columns) {
		return ""
	}
	return fk.Columns[i]
}

// findRootConfig locates the configured transformer to clone for a root: the
// transformer on (schema, name) with the given transformer name whose column
// static parameter equals column. Returns a clone (never a pointer into the
// config slice).
func findRootConfig(
	tableConfigs []core.TableConfig,
	schema, name, transformerName, column string,
) *core.TransformerConfig {
	for i := range tableConfigs {
		tc := &tableConfigs[i]
		if tc.Schema != schema || tc.Name != name {
			continue
		}
		for j := range tc.Transformers {
			trc := &tc.Transformers[j]
			if trc.Name == transformerName && string(trc.StaticParams["column"]) == column {
				return trc.Clone()
			}
		}
	}
	return nil
}

// columnTypeOverride returns the configured columns_type_override for a column on
// (schema, name), or "" when none.
func columnTypeOverride(tableConfigs []core.TableConfig, schema, name, column string) string {
	for i := range tableConfigs {
		tc := &tableConfigs[i]
		if tc.Schema != schema || tc.Name != name {
			continue
		}
		return tc.ColumnsTypeOverride[column]
	}
	return ""
}

// explicitTransformerOnColumn returns the name of the first explicit (non-derived)
// transformer affecting the given column, or "" when none. Reference propagation
// defers to any explicit transformer the user placed on the column, regardless of
// its name.
func explicitTransformerOnColumn(transformers []core.TransformerContexter, column string) string {
	for _, tc := range transformers {
		tctx, ok := tc.(*transformercontext.TransformerContext)
		if !ok || tctx.Source.Kind == core.TransformationSourceKindDerived {
			continue
		}
		for _, col := range tctx.GetAffectedColumns() {
			if col == column {
				return tctx.Describe()
			}
		}
	}
	return ""
}

// existingDerivedOnColumn returns the first derived transformer already present
// on the given column, or nil when none. Used to deduplicate a column reached
// through more than one foreign-key path.
func existingDerivedOnColumn(
	transformers []core.TransformerContexter, column string,
) *transformercontext.TransformerContext {
	for _, tc := range transformers {
		tctx, ok := tc.(*transformercontext.TransformerContext)
		if !ok || tctx.Source.Kind != core.TransformationSourceKindDerived {
			continue
		}
		for _, col := range tctx.GetAffectedColumns() {
			if col == column {
				return tctx
			}
		}
	}
	return nil
}

// sameRoot reports whether an existing derived transformer originates from the
// same root as rt (same root object, root column and transformer name). When it
// does, re-applying it is redundant (a diamond); when it does not, the two are a
// genuine conflict on the same column.
func sameRoot(existing *transformercontext.TransformerContext, rt *rootMapping) bool {
	df := existing.Source.DerivedFrom
	if df == nil {
		return false
	}
	return existing.Describe() == rt.transformerName &&
		df.Field.Value == rt.rootColumn &&
		sameEntity(df.Object, rt.rootIdentity)
}

// sameEntity compares two entity identities by their stable key.
func sameEntity(a, b core.EntityIdentity) bool {
	ka, errA := a.StableKey()
	kb, errB := b.StableKey()
	return errA == nil && errB == nil && ka == kb
}

// rewriteWhen retargets record/raw_record column references in an inherited
// when-expression from the parent column to the referencing column.
func rewriteWhen(when, parentColumn, childColumn string) string {
	when = strings.ReplaceAll(when,
		recordNamespace+"."+parentColumn,
		recordNamespace+"."+childColumn)
	when = strings.ReplaceAll(when,
		rawRecordNamespace+"."+parentColumn,
		rawRecordNamespace+"."+childColumn)
	return when
}

// edgeKey builds a stable identifier for an edge, used both by the cycle guard
// and to order the traversal deterministically. It disambiguates multiple foreign
// keys between the same pair of tables. Non-foreign-key edges (which the traversal
// skips) still get a well-defined key from their endpoints.
func edgeKey(e core.ObjectEdge) string {
	constraint := ""
	var cols []string
	if fk, ok := e.Link.Payload.(core.ForeignKeyLinkPayload); ok {
		constraint = fk.ConstraintName
		cols = fk.Columns
	}
	return fmt.Sprintf("%d->%d:%s:%s", e.From, e.To, constraint, strings.Join(cols, ","))
}

// sortedActiveColumns returns the keys of an active-mapping set in a stable,
// sorted order for deterministic iteration (see Derive for why determinism
// matters to the snapshot).
func sortedActiveColumns(active map[string][]*rootMapping) []string {
	cols := make([]string, 0, len(active))
	for col := range active {
		cols = append(cols, col)
	}
	slices.Sort(cols)
	return cols
}

func sliceToSet(s []string) map[string]struct{} {
	set := make(map[string]struct{}, len(s))
	for _, v := range s {
		set[v] = struct{}{}
	}
	return set
}
