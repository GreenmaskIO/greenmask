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

import "errors"

type Reference struct {
	Idx    int
	Schema string
	Name   string
	// ReferencedKeys - list of foreign keys of current table
	ReferencedKeys []string
	IsNullable     bool
}

type Table struct {
	Schema      string       `json:"schema"`
	Name        string       `json:"name"`
	Oid         Oid          `json:"oid"`
	Columns     []*Column    `json:"columns"`
	Kind        string       `json:"kind"`
	Parent      Oid          `json:"parent"`
	Children    []Oid        `json:"children"`
	Size        int64        `json:"size"`
	PrimaryKey  []string     `json:"primary_key"`
	Constraints []Constraint `json:"-"`
}

func (t *Table) Validate() error {
	if t.Schema == "" {
		return errors.New("empty table schema")
	}
	if t.Name == "" {
		return errors.New("empty table name")
	}
	if t.Oid == 0 {
		return errors.New("empty table oid")
	}
	if len(t.Columns) == 0 {
		return errors.New("empty table columns")
	}

	return nil
}
