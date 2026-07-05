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

package processor

import (
	"context"
	"time"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

const taskCompletionPollInterval = 500 * time.Millisecond

// specProducer feeds ObjectRestoreSpecs into ch and closes it when done.
// Workers call mapper.SetTaskCompleted after each spec finishes so that an
// orderedProducer can gate the next dispatch on its dependency set.
type specProducer interface {
	Produce(ctx context.Context, ch chan<- core.ObjectRestoreSpec) error
}

// --- unorderedProducer ---

type unorderedProducer struct {
	specs []core.ObjectRestoreSpec
}

func (p *unorderedProducer) Produce(ctx context.Context, ch chan<- core.ObjectRestoreSpec) error {
	defer close(ch)
	for _, spec := range p.specs {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- spec:
		}
	}
	return nil
}

// --- orderedProducer ---

// orderedProducer walks RestorationOrder and dispatches each spec only after
// all its declared dependencies have been marked completed in mapper.
// This mirrors the V1 ProducerWithOrder: the worker pool stays fully parallel;
// only the dispatch is gated, so independent FK branches run concurrently.
type orderedProducer struct {
	specs  []core.ObjectRestoreSpec
	order  []core.TaskID
	deps   map[core.TaskID][]core.TaskID
	mapper core.TaskMapper
}

func (p *orderedProducer) Produce(ctx context.Context, ch chan<- core.ObjectRestoreSpec) error {
	defer close(ch)

	specsByID := make(map[core.TaskID]core.ObjectRestoreSpec, len(p.specs))
	for _, s := range p.specs {
		specsByID[s.TaskID] = s
	}

	for _, taskID := range p.order {
		spec, ok := specsByID[taskID]
		if !ok {
			continue
		}
		if err := p.waitForDeps(ctx, p.deps[taskID]); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ch <- spec:
		}
	}
	return nil
}

func (p *orderedProducer) waitForDeps(ctx context.Context, deps []core.TaskID) error {
	if len(deps) == 0 {
		return nil
	}
	ticker := time.NewTicker(taskCompletionPollInterval)
	defer ticker.Stop()
	for {
		if p.allCompleted(deps) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (p *orderedProducer) allCompleted(deps []core.TaskID) bool {
	for _, dep := range deps {
		if !p.mapper.IsTaskCompleted(dep) {
			return false
		}
	}
	return true
}
