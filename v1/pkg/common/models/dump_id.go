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

package models

import (
	"fmt"
	"strconv"
	"time"
)

var errEmptyDumpID = fmt.Errorf("dump id cannot be empty")

type DumpID string

const (
	DumpIDLatest DumpID = "latest"
)

func NewDumpID() DumpID {
	return DumpID(strconv.FormatInt(time.Now().UnixMilli(), 10))
}

func (d DumpID) Validate() error {
	if d == "" {
		return errEmptyDumpID
	}

	if d == DumpIDLatest {
		return nil
	}
	if _, err := strconv.ParseInt(string(d), 10, 64); err != nil {
		return fmt.Errorf("dump id must int or latest %s: %w", d, err)
	}
	return nil
}
