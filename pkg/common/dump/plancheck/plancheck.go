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

// Package plancheck holds engine-agnostic structural checks over an assembled
// core.DumpPlan. It is a lightweight safety net that runs in the ValidatePlan
// stage, right before execution: it catches plan-assembly invariants and
// user-exclusion reference gaps that would otherwise produce a broken dump.
//
// The package imports only pkg/common/core and pkg/common/transformers/context,
// both engine-neutral, so it honors the pkg/common boundary (no engine import).
package plancheck

import (
	"context"
	"sort"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	transformercontext "github.com/greenmaskio/greenmask/pkg/common/transformers/context"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
)

// Local meta keys for findings that reference a second object or FK attributes.
// There is no core.MetaKey* for these (they are specific to plancheck), so they
// are local constants.
const (
	// metaKeyReferencedObject names the referenced (parent / chain-root) object
	// that is missing from the dump.
	metaKeyReferencedObject = "ReferencedObject"
	// metaKeyConstraintName names the foreign-key constraint behind a coverage gap.
	metaKeyConstraintName = "ConstraintName"
	// metaKeyForeignKeyNullable records whether the offending foreign key is
	// nullable (a nullable FK can hold NULL, so the gap is less severe).
	metaKeyForeignKeyNullable = "ForeignKeyNullable"
)

// ValidatePlan runs all structural checks over the assembled plan, adds any
// warnings to the collector in ctx, and returns core.ErrFatalValidationError if
// any error-severity warning was produced.
//
// The fatal/non-fatal decision is keyed purely off each warning's severity:
// error-severity findings (internal plan-assembly invariants) abort the run;
// warning-severity findings (user-exclusion reference gaps) are surfaced and
// printed but do not abort.
//
// graph is the dependency graph built over the full pre-filter schema; it is the
// authoritative foreign-key source for the referential-coverage check.
func ValidatePlan(ctx context.Context, plan core.DumpPlan, graph core.DependencyGraphResult) error {
	var warnings []*core.ValidationWarning
	warnings = append(warnings, checkRestorationCoversDumpObjects(plan)...)
	warnings = append(warnings, checkForeignKeyCoverage(plan, graph)...)

	if len(warnings) == 0 {
		return nil
	}

	vc := validationcollector.FromContext(ctx)
	vc.Add(warnings...)

	if core.ValidationWarnings(warnings).IsFatal() {
		return core.ErrFatalValidationError
	}
	return nil
}

// checkRestorationCoversDumpObjects verifies that the restoration context accounts
// for every object selected for the dump. An object present in DumpObjectSpecs but
// unknown to the restoration context would be dumped yet never restored — a silent
// data hole. This can only happen if plan assembly itself is buggy, so a violation
// is fatal (error severity) and a corrupt plan must not produce a dump.
//
// Coverage is checked against the restoration context's per-task set
// (TaskDependencies), which holds one entry per task independently of ordering.
// It is deliberately NOT checked against RestorationOrder: that field is a
// topological *ordering* and is intentionally left empty on a cyclic schema (the
// restore then falls back to an unordered producer that still restores every
// object), so absence from the order would not mean the object is dropped. Keying
// coverage off the task set makes the check both ordering- and cycle-agnostic.
func checkRestorationCoversDumpObjects(plan core.DumpPlan) []*core.ValidationWarning {
	covered := make(map[core.TaskID]bool, len(plan.RestorationContext.TaskDependencies))
	for taskID := range plan.RestorationContext.TaskDependencies {
		covered[taskID] = true
	}

	var warnings []*core.ValidationWarning
	for i := range plan.DumpObjectSpecs {
		spec := plan.DumpObjectSpecs[i]
		if covered[spec.TaskID] {
			continue
		}

		schema, name := specTableMeta(spec)
		warnings = append(warnings, core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityError).
			AddMeta(core.MetaKeyTableSchema, schema).
			AddMeta(core.MetaKeyTableName, name).
			SetMsg("dumped object is not covered by the restoration context; it would be dumped but never restored"))
	}
	return warnings
}

// checkForeignKeyCoverage cross-checks the selected dump set against the schema's
// foreign-key edges: for every FK where the referencing (child) object is dumped
// but the referenced (parent) object is not, it emits a finding. This is the core
// referential-hole detector — the child will be restored with a foreign key that
// points at rows never dumped.
//
// Per the locked severity decision this is a user-exclusion reference gap (the
// user filtered the parent out), so it is a warning, not fatal: the run continues
// and the finding is surfaced. A nullable FK is recorded as such in the meta (it
// can legitimately hold NULL); a config knob can later promote non-null gaps to
// fatal.
//
// The graph is built over the full pre-filter schema, so its edges see parents
// that the plan excluded — exactly what the restoration-context builder silently
// drops when the parent is out of scope.
func checkForeignKeyCoverage(plan core.DumpPlan, graph core.DependencyGraphResult) []*core.ValidationWarning {
	dumped := make(map[core.ObjectID]bool, len(plan.DumpObjectSpecs))
	for i := range plan.DumpObjectSpecs {
		dumped[plan.DumpObjectSpecs[i].ObjectID] = true
	}

	// Collect one gap per child→parent pair (a pair can be reached via several FK
	// columns / constraints), then sort on plain fields for stable output — graph
	// edge iteration order is nondeterministic.
	type gap struct {
		childName  string
		parentName string
		nullable   bool
		constraint string
	}
	// refGap keys the dedup set by the (child, parent) object pair.
	type refGap struct {
		child  core.ObjectID
		parent core.ObjectID
	}

	var gaps []gap
	seen := make(map[refGap]bool)
	for _, edges := range graph.ObjectGraph.Edges {
		for _, edge := range edges {
			if edge.Link.Kind != core.ObjectLinkKindForeignKey {
				continue
			}
			child, parent := edge.From, edge.To
			// A referential hole exists only when the referencing child is dumped
			// but its referenced parent is not: the child's FK then points at rows
			// that were never dumped. Every other combination is fine.
			isHole := dumped[child] && !dumped[parent]
			if !isHole {
				continue
			}
			pair := refGap{child: child, parent: parent}
			if seen[pair] {
				continue
			}
			seen[pair] = true

			g := gap{
				childName:  nodeName(graph, child),
				parentName: nodeName(graph, parent),
			}
			if fk, ok := edge.Link.Payload.(core.ForeignKeyLinkPayload); ok {
				g.nullable = fk.IsNullable
				g.constraint = fk.ConstraintName
			}
			gaps = append(gaps, g)
		}
	}

	// Order findings by child then parent name so the output is stable across runs
	// (graph edge iteration order is nondeterministic).
	sort.Slice(gaps, func(i, j int) bool {
		return gaps[i].childName+gaps[i].parentName < gaps[j].childName+gaps[j].parentName
	})

	warnings := make([]*core.ValidationWarning, 0, len(gaps))
	for _, g := range gaps {
		warnings = append(warnings, core.NewValidationWarning().
			SetSeverity(core.ValidationSeverityWarning).
			AddMeta(core.MetaKeyTableName, g.childName).
			AddMeta(metaKeyReferencedObject, g.parentName).
			AddMeta(metaKeyForeignKeyNullable, g.nullable).
			AddMeta(metaKeyConstraintName, g.constraint).
			SetMsg("dumped object has a foreign key to an object excluded from the dump; "+
				"the reference will be unsatisfied on restore"))
	}
	return warnings
}

// nodeName returns the human name of a graph node, or the empty string when the
// node is unknown (should not happen for an edge endpoint).
func nodeName(graph core.DependencyGraphResult, id core.ObjectID) string {
	return graph.ObjectGraph.Nodes[id].Name
}

// specTableMeta extracts the (schema, name) pair for warning metadata. It prefers
// the resolved table on a TableDumpContext payload and falls back to the spec
// identity/name so a spec with an unexpected payload still produces useful meta.
func specTableMeta(spec core.ObjectDumpSpec) (schema, name string) {
	if payload, ok := spec.Payload.(transformercontext.TableDumpContext); ok && payload.Table != nil {
		return payload.Table.Schema, payload.Table.Name
	}
	if n := identityName(spec.Identity); n != "" {
		return "", n
	}
	return "", spec.Name
}

// identityName returns the best-effort human name of an identity, falling back to
// its stable key when the name parts are incomplete.
func identityName(identity core.EntityIdentity) string {
	if name, err := identity.Name(); err == nil {
		return name
	}
	if key, err := identity.StableKey(); err == nil {
		return string(key)
	}
	return ""
}
