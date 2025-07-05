package parameters

import "github.com/greenmaskio/greenmask/v1/internal/common/models"

type Parameterizer interface {
	Name() string
	Value() (value any, err error)
	RawValue() (rawValue models.ParamsValue, err error)
	Scan(dest any) (err error)
	GetDefinition() *ParameterDefinition
	IsDynamic() bool
	IsEmpty() (bool, error)
}
