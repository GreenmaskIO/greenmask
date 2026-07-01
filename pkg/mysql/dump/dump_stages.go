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
	"errors"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/metadatawriter"
	"github.com/greenmaskio/greenmask/pkg/common/dump/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/filterconfig"
	"github.com/greenmaskio/greenmask/pkg/common/graphbuilder"
	"github.com/greenmaskio/greenmask/pkg/common/storageprovisioner"
	"github.com/greenmaskio/greenmask/pkg/common/subsetbuilder"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	kinds "github.com/greenmaskio/greenmask/pkg/mysql/kinds"
)

var (
	errNotImplemented        = fmt.Errorf("mysql dump stage: not implemented")
	errUnsupportedObjectKind = errors.New("unsupported object kind")
)

// NewDumpStages assembles the MySQL implementation of pipeline.DumpStages.
//
// Every stage is wired here so the pipeline never imports DBMS-specific
// packages; only this constructor knows about the concrete MySQL stage types.
// Most stages are currently placeholder stubs (see stages.go) and will be
// replaced incrementally with real MySQL logic.
//
// The introspector scopes introspection to the configured schemas/databases,
// but it takes that scope per-run from the FilterConfig handed to Introspect
// (built by the common FilterConfigBuilder during discovery), so the config is
// not needed to wire the stages here.
// The destination storage is built by the StorageProvisioner stage from config
// and injected into the execution stage (DumpProcessor) at Dump time, so it is
// not a constructor argument here.
func NewDumpStages() pipeline.DumpStages {
	return pipeline.DumpStages{
		ConnectionConfigurerBuilder: &ConnectionConfigurerBuilder{},
		DatabaseSessionBuilder:      &DumpSessionBuilder{},
		Introspector:                NewIntrospectorV2(),
		DependencyGraphBuilder:      graphbuilder.New(kinds.ObjectKindTable),
		DumpMetadataLoader:          &DumpMetadataLoader{},
		SchemaDriftValidator:        &SchemaDriftValidator{},
		SubsetBuilder:               subsetbuilder.New(subsetbuilder.DialectMySQL, kinds.ObjectKindTable),
		ConfigEditor:                &ConfigEditor{},
		ObjectFilter:                NewObjectFilter(),
		FilterConfigBuilder:         filterconfig.New(),
		ExplicitDumpContextBuilder:  NewExplicitDumpContextBuilder(registry.DefaultTransformerRegistry.Core()),
		DerivedDumpContextBuilder:   &DerivedDumpContextBuilder{},
		DumpContextSnapshotBuilder:  NewDumpContextSnapshotBuilder(),
		DumpContextDiffer:           &DumpContextDiffer{},
		DumpContextValidator:        &DumpContextValidator{},
		RestorationContextBuilder:   &RestorationContextBuilder{},
		DumpPlanAssembler:           &DumpPlanAssembler{},
		DumpPlanValidator:           &DumpPlanValidator{},
		StorageProvisioner:          storageprovisioner.New(),
		DumpInstructionBuilder:      &DumpInstructionBuilder{},
		DumpProcessor:               &DumpProcessor{},
		MetadataWriter:              metadatawriter.New(),
	}
}

// NewDumpPipeline builds a dump pipeline backed by the MySQL stages.
func NewDumpPipeline() *pipeline.DumpPipeline {
	return pipeline.NewDumpPipeline(NewDumpStages(), core.DBMSEngineMySQL)
}
