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

package utils

import (
	"context"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

// NewTransformerFunc - make new transformer. This function receives Driver for making some steps for validation or
// anything else. parameters - the map of the parsed parameters, for get an appropriate parameter find it
// in the map by the Name. All those parameters has been defined in the TransformerDefinition object of the transformer
type NewTransformerFunc func(
	ctx context.Context,
	vc *validationcollector.Collector,
	driver commonininterfaces.TableDriver,
	parameters map[string]commonparameters.Parameterizer,
) (commonininterfaces.Transformer, error)

type TransformerDefinition struct {
	Properties      *TransformerProperties            `json:"properties"`
	New             NewTransformerFunc                `json:"-"`
	Parameters      []*parameters.ParameterDefinition `json:"parameters"`
	SchemaValidator SchemaValidationFunc              `json:"-"`
}

func NewTransformerDefinition(
	properties *TransformerProperties, newTransformerFunc NewTransformerFunc,
	parameters ...*parameters.ParameterDefinition,
) *TransformerDefinition {
	return &TransformerDefinition{
		Properties:      properties,
		New:             newTransformerFunc,
		Parameters:      parameters,
		SchemaValidator: DefaultSchemaValidator,
	}
}

func (d *TransformerDefinition) SetSchemaValidator(v SchemaValidationFunc) *TransformerDefinition {
	d.SchemaValidator = v
	return d
}
