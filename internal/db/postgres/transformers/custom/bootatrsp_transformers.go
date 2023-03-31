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

package custom

import (
	"context"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/internal/db/postgres/transformers/utils"
	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

const (
	DefaultValidationTimeout        = 20 * time.Second
	DefaultRowTransformationTimeout = 2 * time.Second
	DefaultAutoDiscoveryTimeout     = 10 * time.Second
)

var defaultRowDriver = &toolkit.RowDriverParams{
	Name:   CsvModeName,
	Params: make(map[string]interface{}),
}

func BootstrapCustomTransformers(ctx context.Context, registry *utils.TransformerRegistry, customTransformers []*TransformerDefinition) (err error) {
	for _, ctd := range customTransformers {
		var td *utils.Definition
		if ctd.Name == "" && !ctd.AutoDiscover {
			return fmt.Errorf("custom transformer without auto discovery must be defined staticly in the config")
		}
		if ctd.Executable == "" {
			return fmt.Errorf(`custom transformer "executable" parameter is required`)
		}

		if ctd.AutoDiscoveryTimeout == 0 {
			ctd.AutoDiscoveryTimeout = DefaultAutoDiscoveryTimeout
		}
		if ctd.ValidationTimeout == 0 {
			ctd.ValidationTimeout = DefaultValidationTimeout
		}
		if ctd.RowTransformationTimeout == 0 {
			ctd.RowTransformationTimeout = DefaultRowTransformationTimeout
		}

		if ctd.Driver == nil {
			ctd.Driver = defaultRowDriver
		}

		if ctd.AutoDiscover {
			// Get custom transformer definition from stdout and override received data with config ctd
			err = func() error {
				args := make([]string, len(ctd.Args))
				copy(args, ctd.Args)
				args = append(args, PrintDefinitionArgName)
				ctx, cancel := context.WithTimeout(ctx, ctd.AutoDiscoveryTimeout)
				defer cancel()
				ctdd, err := GetDynamicTransformerDefinition(ctx, ctd.Executable, args...)
				if err != nil {
					return fmt.Errorf("error getting dynamic transformer definition: %w", err)
				}
				ctd.Name = ctdd.Name
				ctd.Description = ctdd.Description
				ctd.Parameters = ctdd.Parameters
				ctd.Driver = ctdd.Driver
				ctd.ExpectedExitCode = ctdd.ExpectedExitCode
				ctd.Validate = ctdd.Validate
				return nil
			}()
			if err != nil {
				return err
			}
		}

		for _, p := range ctd.Parameters {
			if p.IsColumn && p.ColumnProperties == nil {
				p.SetIsColumn(toolkit.
					NewColumnProperties().
					SetAffected(true),
				)
			}
		}

		td = utils.NewDefinition(
			&utils.TransformerProperties{
				Name:        ctd.Name,
				Description: ctd.Description,
				IsCustom:    true,
			},
			ProduceNewCmdTransformerFunction(ctd),
			ctd.Parameters...,
		)

		registry.MustRegister(td)
	}
	return nil
}
