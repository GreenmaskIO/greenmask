package parameters

import "github.com/greenmaskio/greenmask/pkg/toolkit"

type Parameterizer interface {
	Value() (value any, err error)
	RawValue() (rawValue toolkit.ParamsValue, err error)
	Scan(dest any) (empty bool, err error)
	GetDefinition() *toolkit.ParameterDefinition
}
