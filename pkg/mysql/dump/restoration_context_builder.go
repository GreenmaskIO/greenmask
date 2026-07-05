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

package dump

import (
	"context"
	"slices"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.RestorationContextBuilder = (*RestorationContextBuilder)(nil)

// RestorationContextBuilder builds restoration ordering and dependency metadata.
type RestorationContextBuilder struct{}

func (s *RestorationContextBuilder) Build(_ context.Context, input core.RestorationContextInput) (core.RestorationContext, error) {
	if len(input.DumpContext.DumpObjectSpecs) == 0 {
		return core.RestorationContext{}, nil
	}

	// Index specs: ObjectID → TaskID.
	objectIDToTaskID := make(map[core.ObjectID]core.TaskID, len(input.DumpContext.DumpObjectSpecs))
	inScopeIDs := make(map[core.ObjectID]struct{}, len(input.DumpContext.DumpObjectSpecs))
	for _, spec := range input.DumpContext.DumpObjectSpecs {
		objectIDToTaskID[spec.ObjectID] = spec.TaskID
		inScopeIDs[spec.ObjectID] = struct{}{}
	}

	// Cycle detection is delegated to the graph: uses pre-computed SCC info in the
	// condensed graph to avoid re-running DFS here.
	hasTopologicalOrder := !input.DependencyGraph.HasCyclesFor(inScopeIDs)

	// Build task-level dependency graph from ObjectGraph edges.
	// ObjectGraph.Edges[A] contains outgoing edges from A; edge.To is the FK target.
	// A depends on edge.To → A must be restored after edge.To.
	taskDependencies := make(map[core.TaskID][]core.TaskID, len(objectIDToTaskID))
	// dependents[B] = all tasks that depend on B (needed for Kahn's propagation step).
	dependents := make(map[core.TaskID][]core.TaskID, len(objectIDToTaskID))
	for oid, taskID := range objectIDToTaskID {
		taskDependencies[taskID] = []core.TaskID{}
		for _, edge := range input.DependencyGraph.ObjectGraph.Edges[oid] {
			depTaskID, ok := objectIDToTaskID[edge.To]
			if !ok {
				continue
			}
			taskDependencies[taskID] = append(taskDependencies[taskID], depTaskID)
			dependents[depTaskID] = append(dependents[depTaskID], taskID)
		}
	}

	// Kahn's topological sort — used purely for ordering, not cycle detection.
	// inDegree[t] = number of unresolved dependencies for task t.
	inDegree := make(map[core.TaskID]int, len(taskDependencies))
	for taskID, deps := range taskDependencies {
		inDegree[taskID] = len(deps)
	}

	queue := make([]core.TaskID, 0, len(inDegree))
	for taskID, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, taskID)
		}
	}
	slices.Sort(queue)

	result := make([]core.TaskID, 0, len(inDegree))
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		result = append(result, cur)
		for _, t := range dependents[cur] {
			inDegree[t]--
			if inDegree[t] == 0 {
				// Insert in sorted position for determinism.
				idx, _ := slices.BinarySearch(queue, t)
				queue = slices.Insert(queue, idx, t)
			}
		}
	}

	// When cycles exist there is no valid topological order.
	// RestorationOrder is left nil so callers (ProducerWithOrder) do not
	// attempt an ordered restore that would deadlock on the cyclic edges.
	if !hasTopologicalOrder {
		return core.RestorationContext{
			HasTopologicalOrder: false,
			TaskDependencies:    taskDependencies,
		}, nil
	}

	return core.RestorationContext{
		HasTopologicalOrder: true,
		RestorationOrder:    result,
		TaskDependencies:    taskDependencies,
	}, nil
}
