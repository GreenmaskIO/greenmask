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

package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
)

var errBoom = errors.New("boom")

func runtimeOf(s *stubSet) *Runtime { return &Runtime{Session: s.session} }

// discoverOK runs a successful Discover and returns the pipeline + state.
func discoverOK(t *testing.T, s *stubSet) (*DumpPipeline, *RunState) {
	t.Helper()
	p := s.pipeline()
	state := p.NewRun(config.Config{})
	require.NoError(t, p.Discover(context.Background(), runtimeOf(s), state))
	return p, state
}

// contextOK runs Discover + BuildContext successfully.
func contextOK(t *testing.T, s *stubSet) (*DumpPipeline, *RunState) {
	t.Helper()
	p, state := discoverOK(t, s)
	require.NoError(t, p.BuildContext(context.Background(), state))
	return p, state
}

// --- Discover ---------------------------------------------------------------

func TestDiscover_runtimeGuards(t *testing.T) {
	s := newStubSet()
	p := s.pipeline()

	t.Run("nil runtime", func(t *testing.T) {
		err := p.Discover(context.Background(), nil, p.NewRun(config.Config{}))
		require.Error(t, err)
		assert.Zero(t, s.introspector.calls)
	})
	t.Run("nil session", func(t *testing.T) {
		err := p.Discover(context.Background(), &Runtime{}, p.NewRun(config.Config{}))
		require.Error(t, err)
	})
}

func TestDiscover_stageErrors(t *testing.T) {
	tests := []struct {
		name   string
		setup  func(s *stubSet)
		assert func(t *testing.T, s *stubSet)
	}{
		{
			name:  "filter config error",
			setup: func(s *stubSet) { s.filterCfg.err = errBoom },
		},
		{
			name:  "introspect error",
			setup: func(s *stubSet) { s.introspector.err = errBoom },
		},
		{
			name:  "dependency graph error",
			setup: func(s *stubSet) { s.graph.err = errBoom },
		},
		{
			name:  "metadata load non-not-found error",
			setup: func(s *stubSet) { s.metaLoader.err = errBoom },
		},
		{
			name:  "subset error",
			setup: func(s *stubSet) { s.subset.err = errBoom },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStubSet()
			tt.setup(s)
			p := s.pipeline()
			state := p.NewRun(config.Config{})

			err := p.Discover(context.Background(), runtimeOf(s), state)
			require.ErrorIs(t, err, errBoom)
			assert.False(t, state.ExecutedStages[StageNameDiscovery])
		})
	}
}

func TestDiscover_previousMetadata(t *testing.T) {
	t.Run("not found: no previous metadata, no drift compare", func(t *testing.T) {
		s := newStubSet()
		s.metaLoader.err = core.ErrPreviousMetadataNotFound
		_, state := discoverOK(t, s)

		assert.Nil(t, state.Discovery.PreviousMetadata)
		assert.Nil(t, state.Discovery.SchemaDrift)
		assert.Zero(t, s.drift.calls, "drift must not be compared without previous metadata")
	})

	t.Run("found: previous metadata kept and drift computed", func(t *testing.T) {
		s := newStubSet()
		s.metaLoader.meta = &core.Metadata{}
		_, state := discoverOK(t, s)

		assert.NotNil(t, state.Discovery.PreviousMetadata)
		assert.NotNil(t, state.Discovery.SchemaDrift)
		assert.Equal(t, 1, s.drift.calls)
	})

	// Regression guard for the dead-store fix: a not-found error must yield a nil
	// PreviousMetadata even if the loader also returns a (stale) metadata value.
	t.Run("not found wins over a returned metadata value", func(t *testing.T) {
		s := newStubSet()
		s.metaLoader.meta = &core.Metadata{}
		s.metaLoader.err = core.ErrPreviousMetadataNotFound
		_, state := discoverOK(t, s)

		assert.Nil(t, state.Discovery.PreviousMetadata)
		assert.Zero(t, s.drift.calls)
	})
}

func TestDiscover_success(t *testing.T) {
	s := newStubSet()
	_, state := discoverOK(t, s)

	assert.True(t, state.ExecutedStages[StageNameDiscovery])
	assert.NotNil(t, state.Discovery.Introspection)
	assert.NotNil(t, state.Discovery.FilterConfig)
	assert.NotNil(t, state.Discovery.DependencyGraph)
	assert.NotNil(t, state.Discovery.Subset)
	assert.Equal(t, 1, s.filterCfg.calls)
	assert.Equal(t, 1, s.introspector.calls)
	assert.Equal(t, 1, s.graph.calls)
	assert.Equal(t, 1, s.subset.calls)
}

// --- BuildContext -----------------------------------------------------------

func TestBuildContext_requiresDiscovery(t *testing.T) {
	s := newStubSet()
	p := s.pipeline()
	err := p.BuildContext(context.Background(), p.NewRun(config.Config{}))
	require.Error(t, err)
	assert.Zero(t, s.filterCfg.calls)
}

func TestBuildContext_stageErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(s *stubSet)
	}{
		{"object filter error", func(s *stubSet) { s.objFilter.err = errBoom }},
		{"explicit context error", func(s *stubSet) { s.explicit.err = errBoom }},
		{"derived context error", func(s *stubSet) { s.derived.err = errBoom }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStubSet()
			tt.setup(s)
			p, state := discoverOK(t, s)

			err := p.BuildContext(context.Background(), state)
			require.ErrorIs(t, err, errBoom)
			assert.False(t, state.ExecutedStages[StageNameContextBuilding])
		})
	}
}

func TestBuildContext_success(t *testing.T) {
	s := newStubSet()
	_, state := contextOK(t, s)

	assert.True(t, state.ExecutedStages[StageNameContextBuilding])
	assert.NotNil(t, state.Context.FinalCtx)
	assert.NotNil(t, state.Context.ExplicitCtx)
	assert.Equal(t, 1, s.cfgEditor.calls)
	assert.Equal(t, 1, s.objFilter.calls)
	assert.Equal(t, 1, s.explicit.calls)
	assert.Equal(t, 1, s.derived.calls)
}

// --- BuildSnapshotAndDiff ----------------------------------------------------

func TestBuildSnapshotAndDiff_requiresContext(t *testing.T) {
	s := newStubSet()
	p, state := discoverOK(t, s) // context building not run
	err := p.BuildSnapshotAndDiff(context.Background(), state)
	require.Error(t, err)
	assert.Zero(t, s.snapshot.calls)
}

func TestBuildSnapshotAndDiff_stageErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(s *stubSet)
	}{
		{"snapshot error", func(s *stubSet) { s.snapshot.err = errBoom }},
		{"diff error", func(s *stubSet) { s.differ.err = errBoom }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStubSet()
			tt.setup(s)
			p, state := contextOK(t, s)

			err := p.BuildSnapshotAndDiff(context.Background(), state)
			require.ErrorIs(t, err, errBoom)
			assert.False(t, state.ExecutedStages[StageNameSnapshotDiffBuilding])
		})
	}
}

func TestBuildSnapshotAndDiff_previousSnapshotWiring(t *testing.T) {
	t.Run("no previous metadata: differ receives nil previous", func(t *testing.T) {
		s := newStubSet()
		s.metaLoader.err = core.ErrPreviousMetadataNotFound
		p, state := contextOK(t, s)
		require.NoError(t, p.BuildSnapshotAndDiff(context.Background(), state))

		assert.Nil(t, s.differ.gotInput.Previous)
	})

	t.Run("previous metadata present: differ receives non-nil previous", func(t *testing.T) {
		s := newStubSet()
		s.metaLoader.meta = &core.Metadata{}
		p, state := contextOK(t, s)
		require.NoError(t, p.BuildSnapshotAndDiff(context.Background(), state))

		assert.NotNil(t, s.differ.gotInput.Previous)
		assert.True(t, state.ExecutedStages[StageNameSnapshotDiffBuilding])
	})
}

// --- ValidateContext --------------------------------------------------------

func TestValidateContext_requirements(t *testing.T) {
	t.Run("missing context building", func(t *testing.T) {
		s := newStubSet()
		p, state := discoverOK(t, s)
		err := p.ValidateContext(context.Background(), state)
		require.Error(t, err)
	})

	// Guard for the requirements fix: reaching ValidateContext without
	// BuildSnapshotAndDiff must return a clean error, not a nil-dereference panic.
	t.Run("missing snapshot/diff returns error, not panic", func(t *testing.T) {
		s := newStubSet()
		p, state := contextOK(t, s)
		err := p.ValidateContext(context.Background(), state)
		require.Error(t, err)
		assert.Zero(t, s.ctxValidator.calls)
	})
}

func TestValidateContext_validatorError(t *testing.T) {
	s := newStubSet()
	s.ctxValidator.err = errBoom
	p, state := contextOK(t, s)
	require.NoError(t, p.BuildSnapshotAndDiff(context.Background(), state))

	err := p.ValidateContext(context.Background(), state)
	require.ErrorIs(t, err, errBoom)
	assert.False(t, state.ExecutedStages[StageNameContextValidation])
}

func TestValidateContext_success(t *testing.T) {
	s := newStubSet()
	p, state := contextOK(t, s)
	require.NoError(t, p.BuildSnapshotAndDiff(context.Background(), state))
	require.NoError(t, p.ValidateContext(context.Background(), state))

	assert.True(t, state.ExecutedStages[StageNameContextValidation])
	assert.Equal(t, 1, s.ctxValidator.calls)
}

// --- BuildPlan / ValidatePlan -----------------------------------------------

// planReady advances a state through everything BuildPlan requires.
func planReady(t *testing.T, s *stubSet) (*DumpPipeline, *RunState) {
	t.Helper()
	p, state := contextOK(t, s)
	require.NoError(t, p.BuildSnapshotAndDiff(context.Background(), state))
	require.NoError(t, p.ValidateContext(context.Background(), state))
	return p, state
}

func TestBuildPlan_requirements(t *testing.T) {
	s := newStubSet()
	p, state := contextOK(t, s) // snapshot/diff + context validation not run
	err := p.BuildPlan(context.Background(), state)
	require.Error(t, err)
	assert.Zero(t, s.restoration.calls)
}

func TestBuildPlan_stageErrors(t *testing.T) {
	tests := []struct {
		name  string
		setup func(s *stubSet)
	}{
		{"restoration context error", func(s *stubSet) { s.restoration.err = errBoom }},
		{"plan assemble error", func(s *stubSet) { s.planAssembler.err = errBoom }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newStubSet()
			tt.setup(s)
			p, state := planReady(t, s)

			err := p.BuildPlan(context.Background(), state)
			require.ErrorIs(t, err, errBoom)
			assert.False(t, state.ExecutedStages[StageNamePlanBuilding])
		})
	}
}

func TestBuildPlan_success(t *testing.T) {
	s := newStubSet()
	p, state := planReady(t, s)
	require.NoError(t, p.BuildPlan(context.Background(), state))

	assert.True(t, state.ExecutedStages[StageNamePlanBuilding])
	assert.NotNil(t, state.BuildPlan.Plan)
	assert.Equal(t, 1, s.restoration.calls)
	assert.Equal(t, 1, s.planAssembler.calls)
}

func TestValidatePlan(t *testing.T) {
	t.Run("requires plan building", func(t *testing.T) {
		s := newStubSet()
		p, state := planReady(t, s)
		err := p.ValidatePlan(context.Background(), state)
		require.Error(t, err)
		assert.Zero(t, s.planValidator.calls)
	})

	t.Run("validator error", func(t *testing.T) {
		s := newStubSet()
		s.planValidator.err = errBoom
		p, state := planReady(t, s)
		require.NoError(t, p.BuildPlan(context.Background(), state))

		err := p.ValidatePlan(context.Background(), state)
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("success", func(t *testing.T) {
		s := newStubSet()
		p, state := planReady(t, s)
		require.NoError(t, p.BuildPlan(context.Background(), state))
		require.NoError(t, p.ValidatePlan(context.Background(), state))

		assert.True(t, state.ExecutedStages[StageNamePlanValidation])
		assert.Equal(t, 1, s.planValidator.calls)
	})
}

// --- Execute ----------------------------------------------------------------

func TestExecute(t *testing.T) {
	planValidated := func(t *testing.T, s *stubSet) (*DumpPipeline, *RunState) {
		t.Helper()
		p, state := planReady(t, s)
		require.NoError(t, p.BuildPlan(context.Background(), state))
		require.NoError(t, p.ValidatePlan(context.Background(), state))
		return p, state
	}

	t.Run("requires plan validation", func(t *testing.T) {
		s := newStubSet()
		p, state := planReady(t, s)
		require.NoError(t, p.BuildPlan(context.Background(), state))
		// plan validation intentionally skipped
		err := p.Execute(context.Background(), runtimeOf(s), state)
		require.Error(t, err)
		assert.Zero(t, s.processor.calls)
	})

	t.Run("processor error", func(t *testing.T) {
		s := newStubSet()
		s.processor.err = errBoom
		p, state := planValidated(t, s)
		err := p.Execute(context.Background(), runtimeOf(s), state)
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("success", func(t *testing.T) {
		s := newStubSet()
		p, state := planValidated(t, s)
		require.NoError(t, p.Execute(context.Background(), runtimeOf(s), state))

		assert.True(t, state.ExecutedStages[StageNameExecution])
		assert.NotNil(t, state.ExecuteStage.Metadata)
		assert.Equal(t, 1, s.processor.calls)
	})
}

// --- OpenRuntime / withRuntime lifecycle ------------------------------------

func TestOpenRuntime(t *testing.T) {
	t.Run("connection configurer error", func(t *testing.T) {
		s := newStubSet()
		s.connBuilder.err = errBoom
		_, err := s.pipeline().OpenRuntime(context.Background(), config.Config{})
		require.ErrorIs(t, err, errBoom)
		assert.Zero(t, s.sessBuilder.calls)
	})

	t.Run("session open error", func(t *testing.T) {
		s := newStubSet()
		s.sessBuilder.err = errBoom
		_, err := s.pipeline().OpenRuntime(context.Background(), config.Config{})
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("success", func(t *testing.T) {
		s := newStubSet()
		rt, err := s.pipeline().OpenRuntime(context.Background(), config.Config{})
		require.NoError(t, err)
		assert.NotNil(t, rt.Session)
	})
}

// A failing session Close is logged but does not fail the run, and does not mask
// the run's own (nil) error.
func TestWithRuntime_closeErrorIsSwallowed(t *testing.T) {
	s := newStubSet()
	s.session.closeErr = errBoom
	state, err := s.pipeline().RunShowSchemaDrift(context.Background(), config.Config{})
	require.NoError(t, err)
	assert.True(t, state.ExecutedStages[StageNameDiscovery])
	assert.Equal(t, 1, s.session.closeCalls)
}

// --- RunDump (full pipeline) ------------------------------------------------

func TestRunDump_happyPath(t *testing.T) {
	s := newStubSet()
	state, err := s.pipeline().RunDump(context.Background(), config.Config{})
	require.NoError(t, err)
	require.NotNil(t, state)

	// Every stage ran exactly once and the session was closed afterwards.
	for _, c := range []int{
		s.introspector.calls, s.graph.calls, s.subset.calls,
		s.explicit.calls, s.derived.calls, s.snapshot.calls, s.differ.calls,
		s.ctxValidator.calls, s.restoration.calls, s.planAssembler.calls,
		s.planValidator.calls, s.processor.calls,
	} {
		assert.Equal(t, 1, c)
	}
	assert.Equal(t, 1, s.session.closeCalls)
	assert.True(t, state.ExecutedStages[StageNameExecution])
}

func TestRunDump_errorPropagatesAndClosesSession(t *testing.T) {
	s := newStubSet()
	s.subset.err = errBoom // fails during discovery, mid-run
	state, err := s.pipeline().RunDump(context.Background(), config.Config{})

	require.ErrorIs(t, err, errBoom)
	require.NotNil(t, state, "RunDump returns state on failure so warnings are readable")
	assert.Equal(t, 1, s.session.closeCalls, "session must be closed even on error")
	assert.Zero(t, s.processor.calls, "later stages must not run")
}

func TestRunDump_warningsCarryStageMeta(t *testing.T) {
	s := newStubSet()
	// Raise a warning from inside the discovery stage via the ctx collector.
	s.introspector.onCtx = func(ctx context.Context) {
		validationcollector.FromContext(ctx).Add(
			core.NewValidationWarning().SetMsg("discovery hiccup"),
		)
	}

	state, err := s.pipeline().RunDump(context.Background(), config.Config{})
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Len(t, state.Warnings, 1)

	w := state.Warnings[0]
	assert.Equal(t, StageNameDiscovery, w.Meta[core.MetaKeyStage],
		"warning must be tagged with the stage it was produced in")
}

func TestRunDump_openRuntimeError(t *testing.T) {
	s := newStubSet()
	s.sessBuilder.err = errBoom
	state, err := s.pipeline().RunDump(context.Background(), config.Config{})
	require.ErrorIs(t, err, errBoom)
	require.NotNil(t, state, "state is returned even when the session fails to open")
	assert.Zero(t, s.session.closeCalls, "no session to close when open failed")
}

// --- convenience Run* methods -----------------------------------------------

func TestRunValidateConfig(t *testing.T) {
	t.Run("success runs discovery and context building only", func(t *testing.T) {
		s := newStubSet()
		state, err := s.pipeline().RunValidateConfig(context.Background(), config.Config{})
		require.NoError(t, err)
		assert.True(t, state.ExecutedStages[StageNameContextBuilding])
		assert.False(t, state.ExecutedStages[StageNameSnapshotDiffBuilding])
		assert.Equal(t, 1, s.session.closeCalls)
	})

	t.Run("error returns state", func(t *testing.T) {
		s := newStubSet()
		s.objFilter.err = errBoom
		state, err := s.pipeline().RunValidateConfig(context.Background(), config.Config{})
		require.ErrorIs(t, err, errBoom)
		require.NotNil(t, state)
		assert.Equal(t, 1, s.session.closeCalls)
	})
}

func TestRunShowSchemaDrift(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s := newStubSet()
		state, err := s.pipeline().RunShowSchemaDrift(context.Background(), config.Config{})
		require.NoError(t, err)
		assert.True(t, state.ExecutedStages[StageNameDiscovery])
	})

	// Documents the (intentional) contract: this method returns the partial state
	// alongside the error, unlike RunDump which returns nil.
	t.Run("returns partial state on error", func(t *testing.T) {
		s := newStubSet()
		s.introspector.err = errBoom
		state, err := s.pipeline().RunShowSchemaDrift(context.Background(), config.Config{})
		require.ErrorIs(t, err, errBoom)
		assert.NotNil(t, state)
		assert.Equal(t, 1, s.session.closeCalls)
	})
}

func TestRunShowDumpDiff(t *testing.T) {
	s := newStubSet()
	state, err := s.pipeline().RunShowDumpDiff(context.Background(), config.Config{})
	require.NoError(t, err)
	assert.True(t, state.ExecutedStages[StageNameSnapshotDiffBuilding])
	assert.NotNil(t, state.BuildSnapshotAndDiff.DumpContextDiff)
	assert.False(t, state.ExecutedStages[StageNameContextValidation])
}

func TestRunValidateContext(t *testing.T) {
	t.Run("requires context building", func(t *testing.T) {
		s := newStubSet()
		p := s.pipeline()
		err := p.RunValidateContext(context.Background(), p.NewRun(config.Config{}))
		require.Error(t, err)
	})

	t.Run("success on a context-built state", func(t *testing.T) {
		s := newStubSet()
		p, state := contextOK(t, s)
		require.NoError(t, p.RunValidateContext(context.Background(), state))
		assert.True(t, state.ExecutedStages[StageNameContextValidation])
	})

	t.Run("snapshot/diff error is wrapped", func(t *testing.T) {
		s := newStubSet()
		s.snapshot.err = errBoom
		p, state := contextOK(t, s)
		require.ErrorIs(t, p.RunValidateContext(context.Background(), state), errBoom)
	})

	t.Run("validation error is wrapped", func(t *testing.T) {
		s := newStubSet()
		s.ctxValidator.err = errBoom
		p, state := contextOK(t, s)
		require.ErrorIs(t, p.RunValidateContext(context.Background(), state), errBoom)
	})
}

func TestRunValidatePlan(t *testing.T) {
	t.Run("requires context building", func(t *testing.T) {
		s := newStubSet()
		p := s.pipeline()
		err := p.RunValidatePlan(context.Background(), p.NewRun(config.Config{}))
		require.Error(t, err)
	})

	t.Run("success runs through plan validation", func(t *testing.T) {
		s := newStubSet()
		p, state := contextOK(t, s)
		require.NoError(t, p.RunValidatePlan(context.Background(), state))
		assert.True(t, state.ExecutedStages[StageNamePlanValidation])
	})

	t.Run("stage errors are wrapped", func(t *testing.T) {
		for _, setup := range []func(s *stubSet){
			func(s *stubSet) { s.snapshot.err = errBoom },
			func(s *stubSet) { s.ctxValidator.err = errBoom },
			func(s *stubSet) { s.planAssembler.err = errBoom },
			func(s *stubSet) { s.planValidator.err = errBoom },
		} {
			s := newStubSet()
			setup(s)
			p, state := contextOK(t, s)
			require.ErrorIs(t, p.RunValidatePlan(context.Background(), state), errBoom)
		}
	})
}

func TestRunShowDumpDiff_errorPaths(t *testing.T) {
	for name, setup := range map[string]func(s *stubSet){
		"discovery error": func(s *stubSet) { s.introspector.err = errBoom },
		"context error":   func(s *stubSet) { s.objFilter.err = errBoom },
		"snapshot error":  func(s *stubSet) { s.snapshot.err = errBoom },
	} {
		t.Run(name, func(t *testing.T) {
			s := newStubSet()
			setup(s)
			state, err := s.pipeline().RunShowDumpDiff(context.Background(), config.Config{})
			require.ErrorIs(t, err, errBoom)
			require.NotNil(t, state)
			assert.Equal(t, 1, s.session.closeCalls)
		})
	}
}
