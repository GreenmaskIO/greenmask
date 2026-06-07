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
	"github.com/greenmaskio/greenmask/pkg/common/dump/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/graphbuilder"
	"github.com/greenmaskio/greenmask/pkg/common/subsetbuilder"
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
func NewDumpStages() pipeline.DumpStages {
	return pipeline.DumpStages{
		ConnectionConfigurerBuilder: &ConnectionConfigurerBuilder{},
		DumpSessionBuilder:          &DumpSessionBuilder{},
		Introspector:                &IntrospectorV2{},
		DependencyGraphBuilder:      graphbuilder.New(),
		DumpMetadataLoader:          &DumpMetadataLoader{},
		SchemaDriftValidator:        &SchemaDriftValidator{},
		SubsetBuilder:               subsetbuilder.New(subsetbuilder.DialectMySQL),
		ConfigEditor:                &ConfigEditor{},
		ExplicitDumpContextBuilder:  &ExplicitDumpContextBuilder{},
		DerivedDumpContextBuilder:   &DerivedDumpContextBuilder{},
		DumpContextSnapshotBuilder:  &DumpContextSnapshotBuilder{},
		DumpContextDiffer:           &DumpContextDiffer{},
		DumpContextValidator:        &DumpContextValidator{},
		RestorationContextBuilder:   &RestorationContextBuilder{},
		DumpPlanAssembler:           &DumpPlanAssembler{},
		DumpPlanValidator:           &DumpPlanValidator{},
		DumpProcessor:               &DumpProcessor{},
	}
}

// NewDumpPipeline builds a dump pipeline backed by the MySQL stages.
func NewDumpPipeline() *pipeline.DumpPipeline {
	return pipeline.NewDumpPipeline(NewDumpStages(), core.DBMSEngineMySQL)
}
