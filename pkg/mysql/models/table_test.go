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

package models

import (
	"testing"

	"github.com/stretchr/testify/assert"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

func strPtr(s string) *string { return &s }
func intPtr(v int) *int       { return &v }

func TestTable_ToCommonTable(t *testing.T) {
	tests := []struct {
		name             string
		col              Column
		wantTypeName     string
		wantFullTypeName string
		wantUnsigned     bool
		wantPrecision    *int
		wantScale        *int
	}{
		{
			name: "signed int: base in TypeName, full in FullTypeName",
			col: Column{
				Name: "a", TypeName: "int", DataType: strPtr("int"),
				NumericPrecision: intPtr(10), NumericScale: intPtr(0),
				TypeID: 1, TypeClass: core.TypeClassInt,
			},
			wantTypeName: "int", wantFullTypeName: "int", wantUnsigned: false,
			wantPrecision: intPtr(10), wantScale: intPtr(0),
		},
		{
			name: "unsigned int: DATA_TYPE base vs COLUMN_TYPE full, Unsigned set",
			col: Column{
				Name: "b", TypeName: "int unsigned", DataType: strPtr("int"),
				NumericPrecision: intPtr(10), NumericScale: intPtr(0),
				TypeID: 1, TypeClass: core.TypeClassInt,
			},
			wantTypeName: "int", wantFullTypeName: "int unsigned", wantUnsigned: true,
			wantPrecision: intPtr(10), wantScale: intPtr(0),
		},
		{
			name: "bigint unsigned zerofill still detected as unsigned",
			col: Column{
				Name: "c", TypeName: "bigint unsigned zerofill", DataType: strPtr("bigint"),
				TypeID: 2, TypeClass: core.TypeClassInt,
			},
			wantTypeName: "bigint", wantFullTypeName: "bigint unsigned zerofill", wantUnsigned: true,
		},
		{
			name: "nil DataType falls back to COLUMN_TYPE for base name",
			col: Column{
				Name: "d", TypeName: "varchar", DataType: nil,
				TypeID: 3, TypeClass: core.TypeClassText,
			},
			wantTypeName: "varchar", wantFullTypeName: "varchar", wantUnsigned: false,
		},
		{
			name: "decimal carries precision and scale",
			col: Column{
				Name: "e", TypeName: "decimal(10,2)", DataType: strPtr("decimal"),
				NumericPrecision: intPtr(10), NumericScale: intPtr(2),
				TypeID: 4, TypeClass: core.TypeClassFloat,
			},
			wantTypeName: "decimal", wantFullTypeName: "decimal(10,2)", wantUnsigned: false,
			wantPrecision: intPtr(10), wantScale: intPtr(2),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbl := &Table{Schema: "s", Name: "t", Columns: []Column{tc.col}}
			common := tbl.ToCommonTable()

			got := common.Columns[0]
			assert.Equal(t, tc.wantTypeName, got.Type.Name, "base Type.Name")
			assert.Equal(t, tc.wantFullTypeName, got.Type.GetFullName(), "Type.GetFullName")
			assert.Equal(t, tc.wantUnsigned, got.Type.Unsigned, "Type.Unsigned")
			assert.Equal(t, tc.wantPrecision, got.Type.Precision, "Type.Precision")
			assert.Equal(t, tc.wantScale, got.Type.Scale, "Type.Scale")
			// IsSigned must reflect the unsigned flag.
			assert.Equal(t, !tc.wantUnsigned, got.Type.IsSigned())
		})
	}
}
