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

// Reference - represents a foreign key constraint.
type Reference struct {
	// ReferencedSchema - schema of the referenced table.
	ReferencedSchema string
	// ReferencedName - name of the referenced table.
	ReferencedName string
	// ConstraintName - name of the foreign key constraint.
	ConstraintName string
	// ConstraintSchema - schema of the foreign key constraint.
	ConstraintSchema string
	// ReferencedKeys - list of the keys that involved in the foreign key constraint.
	Keys []string
	// IsNullable - flag that indicates whether the foreign key constraint is nullable.
	IsNullable bool
}

func NewReference(
	referencedSchema, referencedName, constraintSchema, constraintName string,
	keys []string,
	isNullable bool,
) Reference {
	return Reference{
		ReferencedSchema: referencedSchema,
		ReferencedName:   referencedName,
		ConstraintSchema: constraintSchema,
		ConstraintName:   constraintName,
		Keys:             keys,
		IsNullable:       isNullable,
	}
}

func (r *Reference) SetKeys(keys []string) {
	r.Keys = keys
}
