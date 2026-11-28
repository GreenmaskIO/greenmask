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

var telephoneCleaner = strings.NewReplacer("(", "", ")", "", " ", "", "-", "")

// Telephone removes "(", ")", " " and "-" and masks the last 4 characters.
// Numbers with 10 or 8 characters are formatted as (??)????-**** and ????-****,
// as go-masker does. go-masker returns numbers of any other length unmasked.
func Telephone(v string) string {
	runes := []rune(telephoneCleaner.Replace(v))
	switch len(runes) {
	case 10:
		return "(" + string(runes[:2]) + ")" + string(runes[2:6]) + "-****"
	case 8:
		return string(runes[:4]) + "-****"
	}
	return maskLast4(runes)
}
