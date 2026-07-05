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

// NullValueSeq is the NULL sentinel used in greenmask's textual dump file format.
//
// This is a storage-/wire-format concept, not an engine concept: every engine that
// writes the shared dump format encodes a NULL column as the byte sequence "\N".
// It lives here in core — rather than inside any engine package — so that engine
// writers and readers (and the engine-agnostic rawrecord codec) can share a single
// definition without importing another engine's package, which the architecture
// forbids.
//
// If a future engine needs a genuinely different sentinel, expose it via an interface
// method on that engine's writer/driver rather than redefining a package-level
// constant; this value is the shared default.
var NullValueSeq = []byte("\\N")
