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

package main

import (
	"fmt"
	"runtime/debug"
)

func getVersion(version string) string {
	var (
		commitDate string
		commit     string
	)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				commit = setting.Value
			}
			if setting.Key == "vcs.time" {
				commitDate = setting.Value
			}
		}
	}
	if version != "" {
		return fmt.Sprintf("%s %s %s", version, commit, commitDate)
	}
	return fmt.Sprintf("%s %s", commit, commitDate)
}
