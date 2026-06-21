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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// ── simpleMapper ──────────────────────────────────────────────────────────────
// Thread-safe in-memory TaskMapper used to control orderedProducer dispatch.

type simpleMapper struct {
	mu        sync.Mutex
	completed map[core.TaskID]bool
}

func newSimpleMapper() *simpleMapper {
	return &simpleMapper{completed: make(map[core.TaskID]bool)}
}

func (m *simpleMapper) IsTaskCompleted(id core.TaskID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.completed[id]
}

func (m *simpleMapper) SetTaskCompleted(id core.TaskID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completed[id] = true
}

// drainSpecs reads all specs from ch until it is closed or ctx is done.
func drainSpecs(ctx context.Context, ch <-chan core.ObjectRestoreSpec) []core.ObjectRestoreSpec {
	var out []core.ObjectRestoreSpec
	for {
		select {
		case s, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, s)
		case <-ctx.Done():
			return out
		}
	}
}

// ── unorderedProducer ─────────────────────────────────────────────────────────

func TestUnorderedProducer_allSpecsSent(t *testing.T) {
	specs := []core.ObjectRestoreSpec{
		{TaskID: 1, Kind: "table"},
		{TaskID: 2, Kind: "table"},
		{TaskID: 3, Kind: "table"},
	}
	p := &unorderedProducer{specs: specs}
	ch := make(chan core.ObjectRestoreSpec, len(specs))

	err := p.Produce(context.Background(), ch)
	require.NoError(t, err)

	got := drainSpecs(context.Background(), ch)
	assert.Equal(t, specs, got)
}

func TestUnorderedProducer_emptySpecs(t *testing.T) {
	p := &unorderedProducer{}
	ch := make(chan core.ObjectRestoreSpec)

	err := p.Produce(context.Background(), ch)
	require.NoError(t, err)

	// Channel must be closed by Produce; drain should return immediately.
	got := drainSpecs(context.Background(), ch)
	assert.Empty(t, got)
}

func TestUnorderedProducer_contextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	specs := make([]core.ObjectRestoreSpec, 5)
	for i := range specs {
		specs[i] = core.ObjectRestoreSpec{TaskID: core.TaskID(i + 1)}
	}

	// Unbuffered channel so the producer blocks immediately on the first send.
	ch := make(chan core.ObjectRestoreSpec)

	err := p_produce(ctx, specs, ch)
	// Channel is closed by defer in Produce even on early exit; drain before asserting.
	drainSpecs(context.Background(), ch)

	require.ErrorIs(t, err, context.Canceled)
}

// p_produce is a helper that runs Produce in a goroutine so we can observe the
// returned error without blocking the test goroutine on the unbuffered channel.
func p_produce(ctx context.Context, specs []core.ObjectRestoreSpec, ch chan core.ObjectRestoreSpec) error {
	p := &unorderedProducer{specs: specs}
	errCh := make(chan error, 1)
	go func() { errCh <- p.Produce(ctx, ch) }()
	select {
	case err := <-errCh:
		return err
	case <-time.After(2 * time.Second):
		return nil // shouldn't happen
	}
}

// ── orderedProducer ───────────────────────────────────────────────────────────

func TestOrderedProducer_respectsOrder(t *testing.T) {
	specs := []core.ObjectRestoreSpec{
		{TaskID: 3, Kind: "table"},
		{TaskID: 1, Kind: "table"},
		{TaskID: 2, Kind: "table"},
	}
	mapper := newSimpleMapper()
	// Pre-mark all deps as complete so no waiting occurs.
	mapper.SetTaskCompleted(1)
	mapper.SetTaskCompleted(2)
	mapper.SetTaskCompleted(3)

	p := &orderedProducer{
		specs:  specs,
		order:  []core.TaskID{1, 2, 3},
		deps:   map[core.TaskID][]core.TaskID{},
		mapper: mapper,
	}
	ch := make(chan core.ObjectRestoreSpec, len(specs))

	err := p.Produce(context.Background(), ch)
	require.NoError(t, err)

	got := drainSpecs(context.Background(), ch)
	require.Len(t, got, 3)
	assert.Equal(t, core.TaskID(1), got[0].TaskID)
	assert.Equal(t, core.TaskID(2), got[1].TaskID)
	assert.Equal(t, core.TaskID(3), got[2].TaskID)
}

func TestOrderedProducer_skipsUnknownTaskIDs(t *testing.T) {
	spec := core.ObjectRestoreSpec{TaskID: 1, Kind: "table"}

	p := &orderedProducer{
		specs:  []core.ObjectRestoreSpec{spec},
		order:  []core.TaskID{99, 1, 100}, // 99 and 100 are unknown
		deps:   map[core.TaskID][]core.TaskID{},
		mapper: newSimpleMapper(),
	}
	ch := make(chan core.ObjectRestoreSpec, 10)

	err := p.Produce(context.Background(), ch)
	require.NoError(t, err)

	got := drainSpecs(context.Background(), ch)
	require.Len(t, got, 1)
	assert.Equal(t, core.TaskID(1), got[0].TaskID)
}

func TestOrderedProducer_emptyOrder(t *testing.T) {
	p := &orderedProducer{
		specs:  []core.ObjectRestoreSpec{{TaskID: 1}},
		order:  nil,
		deps:   map[core.TaskID][]core.TaskID{},
		mapper: newSimpleMapper(),
	}
	ch := make(chan core.ObjectRestoreSpec)

	err := p.Produce(context.Background(), ch)
	require.NoError(t, err)

	got := drainSpecs(context.Background(), ch)
	assert.Empty(t, got)
}

func TestOrderedProducer_contextCancellation_whileSending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	specs := []core.ObjectRestoreSpec{{TaskID: 1}}
	p := &orderedProducer{
		specs:  specs,
		order:  []core.TaskID{1},
		deps:   map[core.TaskID][]core.TaskID{},
		mapper: newSimpleMapper(),
	}
	// Unbuffered: send will block until ctx cancellation is observed.
	ch := make(chan core.ObjectRestoreSpec)

	errCh := make(chan error, 1)
	go func() { errCh <- p.Produce(ctx, ch) }()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Produce did not return on context cancellation")
	}
	drainSpecs(context.Background(), ch)
}

// ── allCompleted ──────────────────────────────────────────────────────────────

func TestAllCompleted_allDone(t *testing.T) {
	mapper := newSimpleMapper()
	mapper.SetTaskCompleted(1)
	mapper.SetTaskCompleted(2)

	p := &orderedProducer{mapper: mapper}
	assert.True(t, p.allCompleted([]core.TaskID{1, 2}))
}

func TestAllCompleted_notAllDone(t *testing.T) {
	mapper := newSimpleMapper()
	mapper.SetTaskCompleted(1)

	p := &orderedProducer{mapper: mapper}
	assert.False(t, p.allCompleted([]core.TaskID{1, 2}))
}

func TestAllCompleted_noDeps(t *testing.T) {
	p := &orderedProducer{mapper: newSimpleMapper()}
	assert.True(t, p.allCompleted(nil))
	assert.True(t, p.allCompleted([]core.TaskID{}))
}

// ── waitForDeps ───────────────────────────────────────────────────────────────

func TestWaitForDeps_noDeps(t *testing.T) {
	p := &orderedProducer{mapper: newSimpleMapper()}
	err := p.waitForDeps(context.Background(), nil)
	require.NoError(t, err)
}

func TestWaitForDeps_alreadyCompleted(t *testing.T) {
	mapper := newSimpleMapper()
	mapper.SetTaskCompleted(1)
	mapper.SetTaskCompleted(2)

	p := &orderedProducer{mapper: mapper}
	err := p.waitForDeps(context.Background(), []core.TaskID{1, 2})
	require.NoError(t, err)
}

func TestWaitForDeps_contextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	// Dep 1 is never completed.
	p := &orderedProducer{mapper: newSimpleMapper()}

	err := p.waitForDeps(ctx, []core.TaskID{1})
	require.ErrorIs(t, err, context.Canceled)
}

// TestWaitForDeps_completedAfterOnePoll marks the dep as completed in a
// goroutine so that waitForDeps succeeds after at most one polling tick.
func TestWaitForDeps_completedAfterOnePoll(t *testing.T) {
	mapper := newSimpleMapper()
	p := &orderedProducer{mapper: mapper}

	// Mark the dep complete shortly after the ticker fires at least once.
	go func() {
		time.Sleep(taskCompletionPollInterval + 50*time.Millisecond)
		mapper.SetTaskCompleted(42)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*taskCompletionPollInterval)
	defer cancel()

	err := p.waitForDeps(ctx, []core.TaskID{42})
	require.NoError(t, err)
}
