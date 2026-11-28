// Copyright 2026 Greenmask
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

package maskers

import "strings"

// ID masks the last 4 characters and keeps the length of the value. A value
// with 4 characters or fewer is masked completely.
func ID(v string) string {
	return maskLast4([]rune(v))
}

// maskLast4 replaces the last 4 runes with "*". If there are 4 runes or fewer,
// all of them are replaced.
func maskLast4(runes []rune) string {
	keep := max(len(runes)-4, 0)
	return string(runes[:keep]) + strings.Repeat("*", len(runes)-keep)
}
