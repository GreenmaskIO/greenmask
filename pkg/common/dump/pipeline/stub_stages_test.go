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
	"io"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// This file provides lightweight, configurable stubs for every stage interface
// in DumpStages, plus a stubSet helper that assembles a fully-wired pipeline in
// which every stage succeeds. Individual tests override the one or two stages
// they exercise (usually by setting an `err` field) and assert on call counters.

// --- session / connection --------------------------------------------------

type stubConnConfigurer struct{}

func (stubConnConfigurer) ConnectionConfig() any { return nil }

type stubSession struct {
	closeCalls int
	closeErr   error
}

func (s *stubSession) Close(context.Context) error { s.closeCalls++; return s.closeErr }
func (s *stubSession) RunWithOperationalDB(ctx context.Context, fn func(context.Context, core.DB) error) error {
	return fn(ctx, nil)
}
func (s *stubSession) RunWithEngineResource(ctx context.Context, fn func(context.Context, any) error) error {
	return fn(ctx, nil)
}

type stubConnConfigurerBuilder struct {
	err   error
	calls int
}

func (b *stubConnConfigurerBuilder) Build(any) (core.ConnectionConfigurer, error) {
	b.calls++
	if b.err != nil {
		return nil, b.err
	}
	return stubConnConfigurer{}, nil
}

type stubSessionBuilder struct {
	session *stubSession
	err     error
	calls   int
}

func (b *stubSessionBuilder) Open(context.Context, core.ConnectionConfigurer) (core.DatabaseSession, error) {
	b.calls++
	if b.err != nil {
		return nil, b.err
	}
	return b.session, nil
}

// --- discovery stages -------------------------------------------------------

type stubIntrospector struct {
	result core.IntrospectionResult
	err    error
	calls  int
	// onCtx, when set, is invoked with the stage ctx so tests can e.g. add a
	// warning to the ctx collector from inside the discovery stage.
	onCtx func(context.Context)
}

func (s *stubIntrospector) Introspect(ctx context.Context, _ core.DatabaseSession, _ core.FilterConfig) (core.IntrospectionResult, error) {
	s.calls++
	if s.onCtx != nil {
		s.onCtx(ctx)
	}
	return s.result, s.err
}

type stubGraphBuilder struct {
	result core.DependencyGraphResult
	err    error
	calls  int
}

func (s *stubGraphBuilder) BuildGraph(context.Context, core.IntrospectionResult) (core.DependencyGraphResult, error) {
	s.calls++
	return s.result, s.err
}

type stubMetaLoader struct {
	meta  *core.Metadata
	err   error
	calls int
}

func (s *stubMetaLoader) LoadPrevious(context.Context, core.PreviousMetadataLoadInput) (*core.Metadata, error) {
	s.calls++
	return s.meta, s.err
}

type stubDriftValidator struct {
	result core.SchemaDriftResult
	calls  int
}

func (s *stubDriftValidator) Compare(context.Context, core.SchemaDriftValidatorInput) core.SchemaDriftResult {
	s.calls++
	return s.result
}

type stubSubsetBuilder struct {
	result core.SubsetResult
	err    error
	calls  int
}

func (s *stubSubsetBuilder) BuildSubset(context.Context, core.SubsetBuilderInput) (core.SubsetResult, error) {
	s.calls++
	return s.result, s.err
}

// --- context stages ---------------------------------------------------------

type stubConfigEditor struct {
	result []core.TableConfig
	calls  int
}

func (s *stubConfigEditor) EditConfig(context.Context, core.ConfigEditInput) []core.TableConfig {
	s.calls++
	return s.result
}

type stubFilterConfigBuilder struct {
	result core.FilterConfig
	err    error
	calls  int
}

func (s *stubFilterConfigBuilder) Build(any) (core.FilterConfig, error) {
	s.calls++
	return s.result, s.err
}

type stubObjectFilter struct {
	result core.ObjectFilterResult
	err    error
	calls  int
}

func (s *stubObjectFilter) FilterObjects(context.Context, core.ObjectFilterInput) (core.ObjectFilterResult, error) {
	s.calls++
	return s.result, s.err
}

type stubExplicitBuilder struct {
	result core.DumpContext
	err    error
	calls  int
}

func (s *stubExplicitBuilder) BuildDumpContext(context.Context, core.ExplicitDumpContextInput) (core.DumpContext, error) {
	s.calls++
	return s.result, s.err
}

type stubDerivedBuilder struct {
	result core.DumpContext
	err    error
	calls  int
}

func (s *stubDerivedBuilder) BuildDumpContext(context.Context, core.DerivedDumpContextInput) (core.DumpContext, error) {
	s.calls++
	return s.result, s.err
}

// --- snapshot / diff / validation / plan ------------------------------------

type stubSnapshotBuilder struct {
	result core.DumpContextSnapshot
	err    error
	calls  int
}

func (s *stubSnapshotBuilder) Build(context.Context, core.DumpContext) (core.DumpContextSnapshot, error) {
	s.calls++
	return s.result, s.err
}

type stubDiffer struct {
	result   core.DumpContextDiff
	err      error
	calls    int
	gotInput core.DumpContextDiffInput
}

func (s *stubDiffer) Diff(_ context.Context, in core.DumpContextDiffInput) (core.DumpContextDiff, error) {
	s.calls++
	s.gotInput = in
	return s.result, s.err
}

type stubContextValidator struct {
	err   error
	calls int
}

func (s *stubContextValidator) Validate(context.Context, core.DumpContextValidatorInput) error {
	s.calls++
	return s.err
}

type stubRestorationBuilder struct {
	result core.RestorationContext
	err    error
	calls  int
}

func (s *stubRestorationBuilder) Build(context.Context, core.RestorationContextInput) (core.RestorationContext, error) {
	s.calls++
	return s.result, s.err
}

type stubPlanAssembler struct {
	result core.DumpPlan
	err    error
	calls  int
}

func (s *stubPlanAssembler) Assemble(context.Context, core.DumpPlanInput) (core.DumpPlan, error) {
	s.calls++
	return s.result, s.err
}

type stubPlanValidator struct {
	err   error
	calls int
}

func (s *stubPlanValidator) Validate(context.Context, core.DumpPlanValidationInput) error {
	s.calls++
	return s.err
}

type stubProcessor struct {
	result core.Metadata
	err    error
	calls  int
}

func (s *stubProcessor) Run(_ context.Context, _ core.DumpRunInput) (core.Metadata, error) {
	s.calls++
	return s.result, s.err
}

// stubStorager is a no-op Storager. SubStorage returns itself so that
// Execute's st.SubStorage(dumpID, true) call doesn't panic.
type stubStorager struct{}

func (stubStorager) GetCwd() string  { return "" }
func (stubStorager) Dirname() string { return "" }
func (stubStorager) ListDir(_ context.Context) ([]string, []core.Storager, error) {
	return nil, nil, nil
}
func (stubStorager) GetObject(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (stubStorager) PutObject(_ context.Context, _ string, _ io.Reader) error     { return nil }
func (stubStorager) Delete(_ context.Context, _ ...string) error                  { return nil }
func (stubStorager) DeleteAll(_ context.Context, _ string) error                  { return nil }
func (stubStorager) Exists(_ context.Context, _ string) (bool, error)             { return false, nil }
func (stubStorager) SubStorage(_ string, _ bool) core.Storager                    { return stubStorager{} }
func (stubStorager) Stat(_ string) (*core.StorageObjectStat, error)               { return nil, nil }
func (stubStorager) Ping(_ context.Context) error                                 { return nil }

type stubInstructionBuilder struct {
	result core.DumpInstruction
	err    error
}

func (s *stubInstructionBuilder) Build(context.Context, any) (core.DumpInstruction, error) {
	return s.result, s.err
}

type stubStorageProvisioner struct {
	storage core.Storager
	err     error
	calls   int
}

func (s *stubStorageProvisioner) Provision(context.Context, any) (core.Storager, error) {
	s.calls++
	return s.storage, s.err
}

type stubMetadataWriter struct {
	err   error
	calls int
}

func (s *stubMetadataWriter) Write(context.Context, core.Storager, core.Metadata) error {
	s.calls++
	return s.err
}

// --- assembly ---------------------------------------------------------------

// stubSet holds a pointer to every stage stub so tests can override behavior and
// assert on call counters. All stages default to success.
type stubSet struct {
	connBuilder   *stubConnConfigurerBuilder
	sessBuilder   *stubSessionBuilder
	session       *stubSession
	introspector  *stubIntrospector
	graph         *stubGraphBuilder
	metaLoader    *stubMetaLoader
	drift         *stubDriftValidator
	subset        *stubSubsetBuilder
	cfgEditor     *stubConfigEditor
	filterCfg     *stubFilterConfigBuilder
	objFilter     *stubObjectFilter
	explicit      *stubExplicitBuilder
	derived       *stubDerivedBuilder
	snapshot      *stubSnapshotBuilder
	differ        *stubDiffer
	ctxValidator  *stubContextValidator
	restoration   *stubRestorationBuilder
	planAssembler *stubPlanAssembler
	planValidator *stubPlanValidator
	instrBuilder  *stubInstructionBuilder
	storageProv   *stubStorageProvisioner
	processor     *stubProcessor
	metadataWr    *stubMetadataWriter
}

func newStubSet() *stubSet {
	session := &stubSession{}
	return &stubSet{
		connBuilder:   &stubConnConfigurerBuilder{},
		sessBuilder:   &stubSessionBuilder{session: session},
		session:       session,
		introspector:  &stubIntrospector{},
		graph:         &stubGraphBuilder{},
		metaLoader:    &stubMetaLoader{},
		drift:         &stubDriftValidator{},
		subset:        &stubSubsetBuilder{},
		cfgEditor:     &stubConfigEditor{},
		filterCfg:     &stubFilterConfigBuilder{},
		objFilter:     &stubObjectFilter{},
		explicit:      &stubExplicitBuilder{},
		derived:       &stubDerivedBuilder{},
		snapshot:      &stubSnapshotBuilder{},
		differ:        &stubDiffer{},
		ctxValidator:  &stubContextValidator{},
		restoration:   &stubRestorationBuilder{},
		planAssembler: &stubPlanAssembler{},
		planValidator: &stubPlanValidator{},
		instrBuilder:  &stubInstructionBuilder{},
		storageProv:   &stubStorageProvisioner{storage: stubStorager{}},
		processor:     &stubProcessor{},
		metadataWr:    &stubMetadataWriter{},
	}
}

func (s *stubSet) stages() DumpStages {
	return DumpStages{
		ConnectionConfigurerBuilder: s.connBuilder,
		DatabaseSessionBuilder:      s.sessBuilder,
		Introspector:                s.introspector,
		DependencyGraphBuilder:      s.graph,
		DumpMetadataLoader:          s.metaLoader,
		SchemaDriftValidator:        s.drift,
		SubsetBuilder:               s.subset,
		ConfigEditor:                s.cfgEditor,
		ObjectFilter:                s.objFilter,
		FilterConfigBuilder:         s.filterCfg,
		ExplicitDumpContextBuilder:  s.explicit,
		DerivedDumpContextBuilder:   s.derived,
		DumpContextSnapshotBuilder:  s.snapshot,
		DumpContextDiffer:           s.differ,
		DumpContextValidator:        s.ctxValidator,
		RestorationContextBuilder:   s.restoration,
		DumpPlanAssembler:           s.planAssembler,
		DumpPlanValidator:           s.planValidator,
		DumpInstructionBuilder:      s.instrBuilder,
		StorageProvisioner:          s.storageProv,
		DumpProcessor:               s.processor,
		MetadataWriter:              s.metadataWr,
	}
}

func (s *stubSet) pipeline() *DumpPipeline {
	return NewDumpPipeline(s.stages(), core.DBMSEngineMySQL)
}
