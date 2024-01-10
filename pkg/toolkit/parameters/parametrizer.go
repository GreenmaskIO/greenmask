package parameters

import "github.com/greenmaskio/greenmask/pkg/toolkit"

type Parameterizer interface {
	Init(defs []*toolkit.Parameter, rawValue toolkit.ParamsValue) (warnings toolkit.ValidationWarnings, err error)
	Value() (value any, err error)
	RawValue() (rawValue toolkit.ParamsValue, err error)
	Scan(dest any) (empty bool, err error)
}
