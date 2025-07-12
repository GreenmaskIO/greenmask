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
