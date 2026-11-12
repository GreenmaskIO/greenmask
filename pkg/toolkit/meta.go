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

package toolkit

import (
	"fmt"
	"time"
)

type Meta struct {
	Table               *Table            `json:"table"`
	Parameters          *Parameters       `json:"parameters"`
	Types               []*Type           `json:"types"`
	ColumnsTypeOverride map[string]string `json:"columns_type_override"`
	// Timezone - IANA name of the location timestamptz values are decoded into. Empty means UTC.
	Timezone string `json:"timezone,omitempty"`
}

// TimestamptzLocation loads Timezone. Empty means UTC, e.g. when meta comes from an older greenmask.
func (m *Meta) TimestamptzLocation() (*time.Location, error) {
	loc, err := time.LoadLocation(m.Timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone in meta: %w", err)
	}
	return loc, nil
}

func newDriverFromMeta(m *Meta) (*Driver, ValidationWarnings, error) {
	loc, err := m.TimestamptzLocation()
	if err != nil {
		return nil, nil, err
	}
	driver, warnings, err := NewDriver(m.Table, m.Types, WithTimestamptzLocation(loc))
	if err != nil {
		return nil, nil, fmt.Errorf("error initilizing Driver: %w", err)
	}
	return driver, warnings, nil
}

type Parameters struct {
	Static  StaticParameters  `json:"static,omitempty"`
	Dynamic DynamicParameters `json:"dynamic,omitempty"`
}
