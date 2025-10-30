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

package mocks

import (
	"github.com/stretchr/testify/mock"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
)

type ParametrizerMock struct {
	mock.Mock
}

func NewParametrizerMock() *ParametrizerMock {
	return &ParametrizerMock{}
}

func (p *ParametrizerMock) Name() string {
	args := p.Called()
	return args.String(0)
}

func (p *ParametrizerMock) Value() (value any, err error) {
	args := p.Called()
	return args.Get(0).(any), args.Error(1)
}

func (p *ParametrizerMock) RawValue() (rawValue commonmodels.ParamsValue, err error) {
	args := p.Called()
	return args.Get(0).(commonmodels.ParamsValue), args.Error(1)
}

func (p *ParametrizerMock) Scan(dest any) (err error) {
	args := p.Called(dest)
	return args.Error(0)
}

func (p *ParametrizerMock) GetDefinition() *commonparameters.ParameterDefinition {
	args := p.Called()
	return args.Get(0).(*commonparameters.ParameterDefinition)
}

func (p *ParametrizerMock) IsDynamic() bool {
	args := p.Called()
	return args.Bool(0)
}

func (p *ParametrizerMock) IsEmpty() (bool, error) {
	args := p.Called()
	return args.Bool(0), args.Error(1)
}
