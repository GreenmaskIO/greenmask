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

package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// TransformationFingerprint computes a stable semantic fingerprint over the
// hashed components of a transformation (never over raw, non-stable values).
func TransformationFingerprint(ts TransformationSnapshot) string {
	return HashStrings([]string{
		ts.Name,
		string(ts.Field.Kind),
		ts.Field.Value,
		strconv.Itoa(ts.Position),
		string(ts.Source.Kind),
		ts.ConfigHash,
		ts.StaticParametersHash,
		ts.DynamicParametersHash,
		ts.AffectedColumnsHash,
		ts.Condition,
	})
}

// HashString returns a deterministic hex sha256 of s, or "" for an empty input
// so that omitempty hash fields stay absent when there is nothing to hash.
func HashString(s string) string {
	if s == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// HashMap returns a deterministic hex sha256 over a map, computed from a
// canonical (key-sorted) representation so the result is independent of Go map
// iteration order. An empty/nil map hashes to "".
func HashMap(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	return HashString(canonicalMap(m))
}

// HashStrings returns a deterministic hex sha256 over an ordered list of strings.
// The order is significant (it is NOT sorted): callers pass an already-canonical
// sequence (e.g. attribute signatures in column order).
func HashStrings(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return HashString(strings.Join(parts, "\x1f"))
}

// canonicalMap renders a map as a key-sorted "k=v" string. Values are rendered
// with %v; this is sufficient for the scalar/stringly config and parameter maps
// captured in snapshots.
func canonicalMap(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('\x1f')
		}
		fmt.Fprintf(&b, "%s=%v", k, m[k])
	}
	return b.String()
}
