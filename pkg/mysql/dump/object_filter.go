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

import (
	"context"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/objectfilter"
	mysqlmodels "github.com/greenmaskio/greenmask/pkg/mysql/models"
)

var _ core.ObjectFilter = (*ObjectFilter)(nil)

// mysqlSystemSchemas are excluded by default unless explicitly included via the
// config include lists.
var mysqlSystemSchemas = []string{"information_schema", "mysql", "performance_schema", "sys"}

// ObjectFilter is the MySQL implementation of core.ObjectFilter. It delegates to
// the generic objectfilter.Filter, configured with the MySQL table kind, the
// MySQL system schemas, and an identity resolver for the MySQL table payload.
type ObjectFilter struct {
	filter *objectfilter.Filter
}

// NewObjectFilter builds the MySQL ObjectFilter.
func NewObjectFilter() *ObjectFilter {
	return &ObjectFilter{
		filter: objectfilter.New(objectfilter.Options{
			RelationKinds: []core.ObjectKind{core.ObjectKindTable},
			SystemSchemas: mysqlSystemSchemas,
			Resolve:       resolveTableIdentity,
		}),
	}
}

func (f *ObjectFilter) FilterObjects(
	ctx context.Context,
	input core.ObjectFilterInput,
) (core.ObjectFilterResult, error) {
	return f.filter.FilterObjects(ctx, input)
}

// resolveTableIdentity extracts the schema-qualified identity from the MySQL
// table payload set by the introspector.
func resolveTableIdentity(obj core.Object) (objectfilter.Identity, bool) {
	t, ok := obj.Payload.(*mysqlmodels.Table)
	if !ok {
		return objectfilter.Identity{}, false
	}
	return objectfilter.Identity{Schema: t.Schema, Name: t.Name}, true
}
