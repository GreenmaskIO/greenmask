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
	"context"
)

type Transformer interface {
	Init(ctx context.Context) error
	Done(ctx context.Context) error
	Transform(ctx context.Context, r Recorder) error
	GetAffectedColumns() map[int]string
	Describe() string
	// IsDeterministic reports whether the transformer produces the same output
	// for the same input on every run. It is true when the configured engine
	// resolves to the deterministic/hash generator, true for inherently
	// deterministic transformers (e.g. Hash, Replace, SetNull), and false for
	// random-engine or externally-driven transformers (Cmd, Template, ...).
	//
	// The derived dump context builder reads this to decide whether a
	// transformer flagged apply_for_references may be propagated onto
	// referencing foreign-key columns: only a deterministic transformer
	// reproduces the same value on both sides of the FK.
	IsDeterministic() bool
}
