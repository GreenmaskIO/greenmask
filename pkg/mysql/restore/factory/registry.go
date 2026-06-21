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

// Package factory wires the MySQL-specific restore factories into the generic
// restorefactory registries.
package factory

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/restorefactory"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/factory/data/table"
	schemafactory "github.com/greenmaskio/greenmask/pkg/mysql/restore/factory/schema"
)

// NewObjectRestoreRegistry returns an ObjectRestoreFactoryRegistry pre-loaded
// with all MySQL object-level restore factories.
func NewObjectRestoreRegistry() (core.ObjectRestoreFactoryRegistry, error) {
	reg := restorefactory.NewObjectRestoreFactoryRegistry()
	if err := reg.Register(table.NewFactory()); err != nil {
		return nil, fmt.Errorf("register mysql table restore factory: %w", err)
	}
	return reg, nil
}

// NewSchemaRestoreRegistry returns a SchemaRestoreFactoryRegistry pre-loaded
// with all MySQL schema restore factories.
func NewSchemaRestoreRegistry(cmd utils.CmdProducer) (core.SchemaRestoreFactoryRegistry, error) {
	reg := restorefactory.NewSchemaRestoreFactoryRegistry()
	if err := reg.Register(schemafactory.NewFactory(cmd)); err != nil {
		return nil, fmt.Errorf("register mysql schema restore factory: %w", err)
	}
	return reg, nil
}
