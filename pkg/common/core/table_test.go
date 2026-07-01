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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType_IsSigned(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		want bool
	}{
		{"zero value is signed", Type{}, true},
		{"explicit signed (Unsigned false)", Type{Name: "int", Class: TypeClassInt}, true},
		{"explicit unsigned", Type{Name: "int", Class: TypeClassInt, Unsigned: true}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.typ.IsSigned())
		})
	}
}

func TestType_GetFullName(t *testing.T) {
	tests := []struct {
		name string
		typ  Type
		want string
	}{
		{"full name present", Type{Name: "int", FullName: "int unsigned"}, "int unsigned"},
		{"falls back to base name", Type{Name: "int"}, "int"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.typ.GetFullName())
		})
	}
}

// TestColumn_TypeZeroValueIsSigned locks in the construction invariant that an
// integer column built as a literal defaults to signed, because Type.Unsigned
// has zero == signed.
func TestColumn_TypeZeroValueIsSigned(t *testing.T) {
	c := Column{Name: "id", Type: Type{Name: "int", Class: TypeClassInt}}
	assert.True(t, c.Type.IsSigned())
}
