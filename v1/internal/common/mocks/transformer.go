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
	"context"

	"github.com/stretchr/testify/mock"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
)

var (
	_ commonininterfaces.Transformer = (*TransformerMock)(nil)
)

type TransformerMock struct {
	mock.Mock
}

func NewTransformerMock(
	newFunctionMock func(
		ctx context.Context,
		tableDriver commonininterfaces.TableDriver,
		parameters map[string]commonparameters.Parameterizer,
	) error,
) (*TransformerMock, utils.NewTransformerFunc) {
	tm := &TransformerMock{}
	return tm, func(
		ctx context.Context,
		tableDriver commonininterfaces.TableDriver,
		parameters map[string]commonparameters.Parameterizer,
	) (commonininterfaces.Transformer, error) {
		if err := newFunctionMock(ctx, tableDriver, parameters); err != nil {
			return nil, err
		}
		return tm, nil
	}
}

func (t *TransformerMock) Init(ctx context.Context) error {
	args := t.Called(ctx)
	return args.Error(0)
}

func (t *TransformerMock) Done(ctx context.Context) error {
	args := t.Called(ctx)
	return args.Error(0)
}

func (t *TransformerMock) Transform(ctx context.Context, r commonininterfaces.Recorder) error {
	args := t.Called(ctx, r)
	return args.Error(0)
}

func (t *TransformerMock) GetAffectedColumns() map[int]string {
	args := t.Called()
	return args.Get(0).(map[int]string)
}
