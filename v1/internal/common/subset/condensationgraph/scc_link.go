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

package condensationgraph

// Link - represents a link to the SCC in the condensation Graph.
//
// It uses to represent the left and right parts of the edge in the condensed Graph.
type Link struct {
	// tableID - index of the SCC in. Using this index the SCC can be identified
	// in the condensation Graph or SCC list. Meaning this ID point to the left or right vertex in the edge.
	tableID int
	SCC     SCC
}

// NewLink - creates a new Link instance.
func NewLink(tableID int, c SCC) Link {
	return Link{
		tableID: tableID,
		SCC:     c,
	}
}

// TableID - return the table ID.
func (l Link) TableID() int {
	return l.tableID
}
