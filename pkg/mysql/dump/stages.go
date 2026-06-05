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

package dump

import "fmt"

// The MySQL dump pipeline stages defined in pkg/common/dump/pipeline.DumpStages
// each live in their own file in this package (introspector.go,
// dependency_graph_builder.go, …) and are wired together by NewDumpStages in
// dump_stages.go.
//
// Most stages are currently placeholder stubs: each returns a "not implemented"
// sentinel (or a benign empty result) until the real MySQL logic is ported over.
// They can be replaced incrementally — the surrounding pipeline, constructor, and
// tests do not need to change as each stub is filled in.
//
// All stages live in pkg/mysql for now. Stages that turn out to be
// engine-agnostic (graph building, subsetting, context/plan validation, diffing,
// etc.) are expected to be promoted to a shared common package later.

// errNotImplemented is returned by stage stubs that cannot yet produce a real
// result. Builders return it directly; validators treat absence of logic as a
// no-op pass so the pipeline can be exercised end to end as stages land.
var errNotImplemented = fmt.Errorf("mysql dump stage: not implemented")
