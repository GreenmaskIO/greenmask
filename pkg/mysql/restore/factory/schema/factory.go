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

package schema

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.SchemaRestoreFactory = (*Factory)(nil)

// Factory builds MysqlSchemaRestorer instances from SchemaRestoreSpecs. The
// mysql client vendor-utility provider is injected so the restorer neither
// builds nor executes the command directly.
type Factory struct {
	provider core.VendorUtilityProvider
}

func NewFactory(provider core.VendorUtilityProvider) *Factory {
	return &Factory{provider: provider}
}

func (f *Factory) Kind() core.SchemaObjectKind {
	return core.SchemaObjectKindMysqlDatabase
}

func (f *Factory) New(spec core.SchemaRestoreSpec) (core.SchemaRestorer, error) {
	payload, ok := spec.Payload.(MysqlSchemaPayload)
	if !ok {
		return nil, fmt.Errorf("expected schema.MysqlSchemaPayload payload, got %T", spec.Payload)
	}
	return &MysqlSchemaRestorer{
		provider: f.provider,
		payload:  payload,
	}, nil
}
