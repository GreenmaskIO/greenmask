// Copyright 2023 Greenmask
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

package transformers

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

// scanLimitParam scans a min or max parameter in dynamic mode.
// dest is left unchanged when a static parameter is not set, so it must hold the default.
func scanLimitParam(p toolkit.Parameterizer, dest any) error {
	if !p.IsDynamic() {
		empty, err := p.IsEmpty()
		if err != nil {
			return fmt.Errorf("check param: %w", err)
		}
		if empty {
			return nil
		}
	}
	return p.Scan(dest)
}

func validateNoiseRatios(minRatio, maxRatio float64) toolkit.ValidationWarnings {
	if minRatio <= maxRatio {
		return nil
	}
	return toolkit.ValidationWarnings{
		toolkit.NewValidationWarning().
			SetMsg("min_ratio is greater than max_ratio").
			SetSeverity(toolkit.ErrorValidationSeverity).
			AddMeta("MinRatio", minRatio).
			AddMeta("MaxRatio", maxRatio),
	}
}
