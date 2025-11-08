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

package parameters

import (
	"fmt"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/template"
)

var (
	errLinkedColumnNameNotSet = fmt.Errorf("linked column name is not set")
)

type StaticParameterContext struct {
	*template.TableDriverContext
	linkedColumnName string
}

func NewStaticParameterContext(td commonininterfaces.TableDriver, linkedColumnName string) *StaticParameterContext {
	return &StaticParameterContext{
		TableDriverContext: template.NewTableDriverContext(td),
		linkedColumnName:   linkedColumnName,
	}
}

func (spc *StaticParameterContext) EncodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf(
			"use .EncodeValueByType or .EncodeValueByColumn instead: %w", errLinkedColumnNameNotSet,
		)
	}
	return spc.EncodeValueByColumn(spc.linkedColumnName, v)
}

func (spc *StaticParameterContext) DecodeValue(v any) (any, error) {
	if spc.linkedColumnName == "" {
		return nil, fmt.Errorf(
			"use .DecodeValueByType or .DecodeValueByColumn instead: %w", errLinkedColumnNameNotSet,
		)
	}
	return spc.DecodeValueByColumn(spc.linkedColumnName, v)
}
