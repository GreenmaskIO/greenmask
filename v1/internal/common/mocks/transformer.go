package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"

	commonininterfaces "github.com/greenmaskio/greenmask/v1/internal/common/interfaces"
	commonparameters "github.com/greenmaskio/greenmask/v1/internal/common/transformers/parameters"
	"github.com/greenmaskio/greenmask/v1/internal/common/transformers/utils"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

type TransformerMock struct {
	mock.Mock
}

func NewTransformerMock() (*TransformerMock, utils.NewTransformerFunc) {
	tm := &TransformerMock{}
	return tm, func(
		ctx context.Context,
		vc *validationcollector.Collector,
		driver commonininterfaces.TableDriver,
		parameters map[string]commonparameters.Parameterizer,
	) (commonininterfaces.Transformer, error) {
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
