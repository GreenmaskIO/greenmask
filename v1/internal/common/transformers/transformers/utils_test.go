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

package transformers

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestTransformWithKeepNull(t *testing.T) {
	t.Run("success not null", func(t *testing.T) {
		recorder := mocks.NewRecorderMock()
		tr, _ := mocks.NewTransformerMock(func(ctx context.Context, tableDriver commonininterfaces.TableDriver, parameters map[string]commonparameters.Parameterizer) error {
			return nil
		})
		tr.On("Transform", mock.Anything, recorder).
			Return(nil)

		recorder.On("IsNullByColumnIdx", 1).
			Return(false, nil)
		transformationFunc := TransformWithKeepNull(tr.Transform, 1)
		err := transformationFunc(context.Background(), recorder)
		assert.NoError(t, err)
		recorder.AssertExpectations(t)
		tr.AssertExpectations(t)
	})

	t.Run("success null", func(t *testing.T) {
		recorder := mocks.NewRecorderMock()
		tr, _ := mocks.NewTransformerMock(func(ctx context.Context, tableDriver commonininterfaces.TableDriver, parameters map[string]commonparameters.Parameterizer) error {
			return nil
		})

		recorder.On("IsNullByColumnIdx", 1).
			Return(true, nil)
		transformationFunc := TransformWithKeepNull(tr.Transform, 1)
		err := transformationFunc(context.Background(), recorder)
		assert.NoError(t, err)
		recorder.AssertExpectations(t)
		tr.AssertExpectations(t)
	})

	t.Run("error check is null", func(t *testing.T) {
		recorder := mocks.NewRecorderMock()
		tr, _ := mocks.NewTransformerMock(func(ctx context.Context, tableDriver commonininterfaces.TableDriver, parameters map[string]commonparameters.Parameterizer) error {
			return nil
		})

		recorder.On("IsNullByColumnIdx", 1).
			Return(false, assert.AnError)
		transformationFunc := TransformWithKeepNull(tr.Transform, 1)
		err := transformationFunc(context.Background(), recorder)
		assert.ErrorIs(t, err, assert.AnError)
		assert.ErrorContains(t, err, "unable to scan column value")
		recorder.AssertExpectations(t)
		tr.AssertExpectations(t)
	})

	t.Run("error transformer method", func(t *testing.T) {
		recorder := mocks.NewRecorderMock()
		tr, _ := mocks.NewTransformerMock(func(ctx context.Context, tableDriver commonininterfaces.TableDriver, parameters map[string]commonparameters.Parameterizer) error {
			return nil
		})

		recorder.On("IsNullByColumnIdx", 1).
			Return(false, nil)

		tr.On("Transform", mock.Anything, recorder).
			Return(assert.AnError)
		transformationFunc := TransformWithKeepNull(tr.Transform, 1)
		err := transformationFunc(context.Background(), recorder)
		assert.ErrorIs(t, err, assert.AnError)
		recorder.AssertExpectations(t)
		tr.AssertExpectations(t)
	})
}

func assertPanicParameterDoesNotExists(t *testing.T, paramName string, v any) {
	t.Helper()
	assert.ErrorIs(t, v.(error), commonmodels.ErrCheckTransformerImplementation)
	assert.ErrorContains(t, v.(error), fmt.Sprintf(`parameter "%s" is not found`, paramName))
}

func TestPanicParameterDoesNotExists(t *testing.T) {
	parameterName := "foo"
	defer func() {
		assertPanicParameterDoesNotExists(t, parameterName, recover())
	}()
	panicParameterDoesNotExists(parameterName)
}

func TestGetParameterValueWithName(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		boolParameter := mocks.NewParametrizerMock()
		parameterName := "boolParameter"
		parameters := map[string]commonparameters.Parameterizer{
			parameterName: boolParameter,
		}
		// Column parameter calls
		boolParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(true, dest))
			}).Return(nil)
		res, err := getParameterValueWithName[bool](ctx, parameters, parameterName)
		assert.NoError(t, err)
		assert.Equal(t, res, true)
		require.False(t, vc.HasWarnings())
		boolParameter.AssertExpectations(t)
	})

	t.Run("parameter is not found", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		parameterName := "unknownParameter"
		defer func() {
			assertPanicParameterDoesNotExists(t, parameterName, recover())
		}()
		parameters := map[string]commonparameters.Parameterizer{}
		res, err := getParameterValueWithName[bool](ctx, parameters, parameterName)
		assert.NoError(t, err)
		assert.Equal(t, res, true)
		require.False(t, vc.HasWarnings())
	})

	t.Run("scan error", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		boolParameter := mocks.NewParametrizerMock()
		parameterName := "boolParameter"
		parameters := map[string]commonparameters.Parameterizer{
			parameterName: boolParameter,
		}
		// Column parameter calls
		boolParameter.On("Scan", mock.Anything).
			Return(assert.AnError)
		_, err := getParameterValueWithName[bool](ctx, parameters, parameterName)
		assert.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		require.True(t, vc.IsFatal())
		require.Equal(t, vc.Len(), 1)
		warning := vc.GetWarnings()[0]
		assert.Equal(t, warning.Severity, commonmodels.ValidationSeverityError)
		assert.Equal(t, warning.Msg, "error scanning parameter")
		assert.Equal(t, warning.Meta, map[string]any{
			commonmodels.MetaKeyParameterName: parameterName,
			commonmodels.MetaKeyError:         assert.AnError.Error(),
		})
		boolParameter.AssertExpectations(t)
	})
}

func TestGetColumnParameterValue(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		tableDriver := mocks.NewTableDriverMock()
		columnParameter := mocks.NewParametrizerMock()
		parameterName := "column"
		expectedColumnName := "test"
		parameters := map[string]commonparameters.Parameterizer{
			parameterName: columnParameter,
		}
		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(expectedColumnName, dest))
			}).Return(nil)
		expectedColumn := &commonmodels.Column{
			Name: expectedColumnName,
		}
		tableDriver.On("GetColumnByName", expectedColumnName).
			Return(expectedColumn, nil)
		actualColumnName, actualColumn, err := getColumnParameterValue(ctx, tableDriver, parameters)
		assert.NoError(t, err)
		assert.Equal(t, expectedColumnName, actualColumnName)
		assert.Equal(t, expectedColumn, actualColumn)
		require.False(t, vc.HasWarnings())
		columnParameter.AssertExpectations(t)
	})

	t.Run("unknown column", func(t *testing.T) {
		vc := validationcollector.NewCollector()
		ctx := validationcollector.WithCollector(context.Background(), vc)
		tableDriver := mocks.NewTableDriverMock()
		columnParameter := mocks.NewParametrizerMock()
		parameterName := "column"
		expectedColumnName := "test"
		parameters := map[string]commonparameters.Parameterizer{
			parameterName: columnParameter,
		}
		// Column parameter calls
		columnParameter.On("Scan", mock.Anything).
			Run(func(args mock.Arguments) {
				dest := args.Get(0)
				require.NoError(t, utils.ScanPointer(expectedColumnName, dest))
			}).Return(nil)
		tableDriver.On("GetColumnByName", expectedColumnName).
			Return(nil, commonmodels.ErrUnknownColumnName)
		_, _, err := getColumnParameterValue(ctx, tableDriver, parameters)
		assert.ErrorIs(t, err, commonmodels.ErrFatalValidationError)
		require.True(t, vc.IsFatal())
		require.Equal(t, vc.Len(), 1)
		warning := vc.GetWarnings()[0]
		assert.Equal(t, commonmodels.ValidationSeverityError, warning.Severity)
		assert.Equal(t, commonmodels.ErrUnknownColumnName, warning.Err)
		assert.Equal(t, map[string]any{
			commonmodels.MetaKeyParameterName:  parameterName,
			commonmodels.MetaKeyParameterValue: expectedColumnName,
			commonmodels.MetaKeyError:          commonmodels.ErrUnknownColumnName.Error(),
		}, warning.Meta)
		columnParameter.AssertExpectations(t)
	})
}
