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

// Email keeps the domain and the first 3 characters of the local part, and
// replaces the rest of the local part with "****", e.g. ggw.chang@gmail.com
// becomes ggw****@gmail.com. A value without "@" is masked as a local part.
func Email(v string) string {
	local, domain, found := strings.Cut(v, "@")
	if found {
		domain = "@" + domain
	}
	runes := []rune(local)
	if len(runes) == 0 {
		return v
	}
	return string(runes[:min(len(runes), 3)]) + "****" + domain
}
