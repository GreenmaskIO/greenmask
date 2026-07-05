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

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/dump/derivation"
)

var _ core.DerivedDumpContextBuilder = (*DerivedDumpContextBuilder)(nil)

// DerivedDumpContextBuilder enriches the explicit dump context with derived
// transformers (transformation inheritance via apply_for_references). The
// engine-agnostic algorithm lives in pkg/common/dump/derivation; this builder
// only supplies the MySQL-specific table driver factory and transformer registry.
//
// Snapshot contract: the derivation stamps each propagated transformation's
// TransformerContext.Source with Kind=derived (and DerivedFrom set) and flips a
// previously-raw child's ObjectDumpSpec.Origin to derived. The
// DumpContextSnapshotBuilder is a pure decoder over those two markers, so drift
// detection sees derived transformations distinctly from explicit config.
type DerivedDumpContextBuilder struct {
	deriver *derivation.Deriver
}

// NewDerivedDumpContextBuilder builds the MySQL derived dump context builder.
// registry resolves derived transformer configurations into runtime transformers.
func NewDerivedDumpContextBuilder(registry core.TransformerRegistry) *DerivedDumpContextBuilder {
	return &DerivedDumpContextBuilder{
		deriver: derivation.New(registry, mysqlDriverFactory{}),
	}
}

func (b *DerivedDumpContextBuilder) BuildDumpContext(ctx context.Context, in core.DerivedDumpContextInput) (core.DumpContext, error) {
	return b.deriver.Derive(ctx, in)
}
