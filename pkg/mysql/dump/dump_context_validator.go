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

var _ core.DumpContextValidator = (*DumpContextValidator)(nil)

// DumpContextValidator validates semantic correctness of the dump context.
// The stub is a no-op pass; validation findings flow through validationcollector.
type DumpContextValidator struct{}

func (s *DumpContextValidator) Validate(ctx context.Context, input core.DumpContextValidatorInput) error {
	return nil
}
