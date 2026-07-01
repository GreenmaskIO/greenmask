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

// Type is the engine-agnostic, self-describing descriptor of a column's data
// type. It separates the catalog base type (Name/ID — what to dispatch on) from
// the vendor's declared type (FullName — fidelity) and from the behavior-bearing
// modifiers (Signed/Precision/Scale/Size). This lets engines that reuse one id
// for signed and unsigned integers (MySQL "int" vs "int unsigned") decode
// unambiguously without a combinatorial type catalog.
//
// Dispatch on Name (the base type); only fall back to ID when Name is empty;
// drive behavior from the structured fields; FullName is fidelity only and is
// NEVER parsed for behavior. "Unsigned" is not a separate type — it is the base
// int type with Unsigned:true.
type Type struct {
	// Name — canonical, modifier-free catalog name ("int","decimal","varchar").
	// The AUTHORITATIVE codec dispatch key. One per vendor base type.
	// (MySQL DATA_TYPE)
	Name string `json:"name"`
	// FullName — vendor's fully-declared type, modifiers verbatim
	// ("int unsigned","decimal(10,2)"). Informational only: display, validation
	// messages, round-trip fidelity. NEVER parsed for behavior. (MySQL COLUMN_TYPE)
	FullName string `json:"full_name,omitempty"`
	// ID — engine type id (canonical, base width; dispatch fallback when Name is empty).
	ID TypeID `json:"id"`
	// Class — canonical class (int, float, text, ...).
	Class TypeClass `json:"class"`

	// --- structured modifiers: the behavior-bearing facts ---

	// Unsigned — integer signedness; true for unsigned integers. The zero value
	// is signed, so Type literals (and every non-integer type, where signedness
	// is meaningless) default to signed.
	Unsigned bool `json:"unsigned,omitempty"`
	// Length — declared length, e.g. varchar(255) -> 255; 0 if N/A.
	Length int `json:"length,omitempty"`
	// Size — storage width in bytes, 0 if N/A.
	Size int `json:"size,omitempty"`
	// Precision — fixed-point precision (DECIMAL(p,s)); nil otherwise.
	Precision *int `json:"precision,omitempty"`
	// Scale — fixed-point scale; nil otherwise.
	Scale *int `json:"scale,omitempty"`
}

// IsSigned reports whether the type is a signed integer. The zero value of Type
// (Unsigned:false) is signed, so Type literals default to signed. For
// non-integer types the flag is meaningless and IsSigned is reported true.
func (t Type) IsSigned() bool { return !t.Unsigned }

// GetFullName returns the vendor's declared type string, falling back to the
// canonical base Name when no full string was recorded.
func (t Type) GetFullName() string {
	if t.FullName != "" {
		return t.FullName
	}
	return t.Name
}
