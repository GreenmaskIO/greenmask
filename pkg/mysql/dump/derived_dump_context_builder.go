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
)

var _ core.DerivedDumpContextBuilder = (*DerivedDumpContextBuilder)(nil)

// DerivedDumpContextBuilder enriches the dump context via semantic derivation.
type DerivedDumpContextBuilder struct{}

func (s *DerivedDumpContextBuilder) BuildDumpContext(ctx context.Context, in core.DerivedDumpContextInput) (core.DumpContext, error) {
	// Placeholder: semantic derivation is not implemented yet, so the explicit
	// dump context is passed through unchanged.
	//
	// Snapshot contract: when derivation is implemented and it adds or changes a
	// transformation on a table, it must also append/update the corresponding
	// entry on that payload's TableDumpContext.SnapshotDescriptor with
	// TransformationSource.Kind = derived (and DerivedFrom set). The
	// DumpContextSnapshotBuilder is a pure decoder over the final descriptor, so
	// any derivation not reflected there is invisible to drift detection.
	return in.ExplicitCtx, nil
}
