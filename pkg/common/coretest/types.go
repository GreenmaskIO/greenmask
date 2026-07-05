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

// Package coretest provides the canonical, engine-neutral core.DBMSDriver used
// by the common transformer test harness. Its type vocabulary is anchored on
// core.TypeClass — the engine-agnostic type taxonomy — rather than on any
// vendor's OID space, so every engine can reuse the shared transformer test
// suite unchanged. A new engine adds only its own dbmsdriver fidelity tests in
// its own package; it does not re-implement a fake driver.
package coretest

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

// Canonical logical type names. These are a deliberate, minimal canonical
// vocabulary that every engine can map onto — not a clone of any single
// vendor's type names.
const (
	TypeInt2      = "int2"
	TypeInt4      = "int4"
	TypeInt8      = "int8"
	TypeFloat4    = "float4"
	TypeFloat8    = "float8"
	TypeNumeric   = "numeric"
	TypeBool      = "bool"
	TypeText      = "text"
	TypeBytea     = "bytea"
	TypeTimestamp = "timestamp"
	TypeDate      = "date"
	TypeTime      = "time"
	TypeJson      = "json"
	TypeUuid      = "uuid"
	// TypeOther exercises the core.TypeClassOther escape hatch.
	TypeOther = "other"
)

// Synthetic TypeIDs. They start at 1000 and are dense so they are obviously not
// a real engine OID, yet remain stable for tests that pin a column's TypeID.
const (
	TypeIDInt2 core.TypeID = 1000 + iota
	TypeIDInt4
	TypeIDInt8
	TypeIDFloat4
	TypeIDFloat8
	TypeIDNumeric
	TypeIDBool
	TypeIDText
	TypeIDBytea
	TypeIDTimestamp
	TypeIDDate
	TypeIDTime
	TypeIDJson
	TypeIDUuid
	TypeIDOther
)

// typeEntry is one row of the canonical catalogue: a logical type with its
// stable name, synthetic id, core type class, and std-lib codecs.
type typeEntry struct {
	name   string
	id     core.TypeID
	class  core.TypeClass
	encode func(src any, buf []byte) ([]byte, error)
	decode func(src []byte) (any, error)
	scan   func(src []byte, dest any) error
}

// catalogue is the single source of truth for the canonical type vocabulary.
// Every core.TypeClass except TypeClassUnsupported is represented at least once.
var catalogue = []typeEntry{
	{TypeInt2, TypeIDInt2, core.TypeClassInt, encodeInt64, decodeInt64, scanInt64},
	{TypeInt4, TypeIDInt4, core.TypeClassInt, encodeInt64, decodeInt64, scanInt64},
	{TypeInt8, TypeIDInt8, core.TypeClassInt, encodeInt64, decodeInt64, scanInt64},
	{TypeFloat4, TypeIDFloat4, core.TypeClassFloat, encodeFloat, decodeFloat, scanFloat},
	{TypeFloat8, TypeIDFloat8, core.TypeClassFloat, encodeFloat, decodeFloat, scanFloat},
	{TypeNumeric, TypeIDNumeric, core.TypeClassNumeric, encodeDecimal, decodeDecimal, scanDecimal},
	{TypeBool, TypeIDBool, core.TypeClassBoolean, encodeBool, decodeBool, scanBool},
	{TypeText, TypeIDText, core.TypeClassText, encodeString, decodeString, scanString},
	{TypeBytea, TypeIDBytea, core.TypeClassBinary, encodeBinary, decodeBinary, scanBinary},
	{TypeTimestamp, TypeIDTimestamp, core.TypeClassDateTime, encodeTimestamp, decodeTimestamp, scanTimestamp},
	{TypeDate, TypeIDDate, core.TypeClassDateTime, encodeTimestamp, decodeTimestamp, scanTimestamp},
	{TypeTime, TypeIDTime, core.TypeClassTime, encodeTime, decodeTime, scanTime},
	{TypeJson, TypeIDJson, core.TypeClassJson, encodeJson, decodeJson, scanJson},
	{TypeUuid, TypeIDUuid, core.TypeClassUuid, encodeUUID, decodeUUID, scanUUID},
	{TypeOther, TypeIDOther, core.TypeClassOther, encodeString, decodeString, scanString},
}

var (
	byName = make(map[string]*typeEntry, len(catalogue))
	byID   = make(map[core.TypeID]*typeEntry, len(catalogue))
)

func init() {
	for i := range catalogue {
		e := &catalogue[i]
		if _, dup := byName[e.name]; dup {
			panic(fmt.Sprintf("coretest: duplicate type name %q", e.name))
		}
		if _, dup := byID[e.id]; dup {
			panic(fmt.Sprintf("coretest: duplicate type id %d", e.id))
		}
		byName[e.name] = e
		byID[e.id] = e
	}
}
