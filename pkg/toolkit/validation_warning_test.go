// Copyright 2023 Greenmask
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

package toolkit

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidationWarnings_HasUnresolved(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		var warns ValidationWarnings
		assert.False(t, warns.HasUnresolved(nil))
	})

	t.Run("unresolved warning severity", func(t *testing.T) {
		warns := ValidationWarnings{
			NewValidationWarning().SetSeverity(WarningValidationSeverity).SetMsg("w"),
		}
		assert.True(t, warns.HasUnresolved(nil))
	})

	t.Run("unresolved error severity", func(t *testing.T) {
		warns := ValidationWarnings{
			NewValidationWarning().SetSeverity(ErrorValidationSeverity).SetMsg("e"),
		}
		assert.True(t, warns.HasUnresolved(nil))
	})

	t.Run("info and debug are ignored", func(t *testing.T) {
		warns := ValidationWarnings{
			NewValidationWarning().SetSeverity(InfoValidationSeverity).SetMsg("i"),
			NewValidationWarning().SetSeverity(DebugValidationSeverity).SetMsg("d"),
		}
		assert.False(t, warns.HasUnresolved(nil))
	})

	t.Run("resolved warning is excluded", func(t *testing.T) {
		w := NewValidationWarning().SetSeverity(WarningValidationSeverity).SetMsg("w")
		w.MakeHash()
		warns := ValidationWarnings{w}
		assert.False(t, warns.HasUnresolved([]string{w.Hash}))
	})

	t.Run("one resolved, one unresolved", func(t *testing.T) {
		resolved := NewValidationWarning().SetSeverity(WarningValidationSeverity).SetMsg("resolved")
		resolved.MakeHash()
		unresolved := NewValidationWarning().SetSeverity(WarningValidationSeverity).SetMsg("unresolved")
		warns := ValidationWarnings{resolved, unresolved}
		assert.True(t, warns.HasUnresolved([]string{resolved.Hash}))
	})
}

func TestValidationWarning_MakeHash_StableHashWarningTypes(t *testing.T) {
	newWarning := func(warningType string, unchanged int) *ValidationWarning {
		w := NewValidationWarning().
			SetSeverity(WarningValidationSeverity).
			SetMsg("msg").
			AddMeta("WarningType", warningType).
			AddMeta("SchemaName", "public").
			AddMeta("TableName", "users").
			AddMeta("ColumnName", "email").
			AddMeta("Unchanged", unchanged)
		w.MakeHash()
		return w
	}

	t.Run("counts do not change the hash", func(t *testing.T) {
		for _, wt := range stableHashWarningTypes {
			assert.Equal(t, newWarning(wt, 1).Hash, newWarning(wt, 2).Hash)
		}
	})

	t.Run("column changes the hash", func(t *testing.T) {
		a := newWarning(UnchangedRowsPercentageThresholdExceededWarningType, 1)
		b := newWarning(UnchangedRowsPercentageThresholdExceededWarningType, 1)
		b.AddMeta("ColumnName", "name").MakeHash()
		assert.NotEqual(t, a.Hash, b.Hash)
	})

	t.Run("other warning types hash all meta", func(t *testing.T) {
		assert.NotEqual(t, newWarning("Other", 1).Hash, newWarning("Other", 2).Hash)
	})
}
