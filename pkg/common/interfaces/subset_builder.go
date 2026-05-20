package interfaces

import commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"

type SubsetBuilder interface {
	BuildSubset(
		in commonmodels.SubsetBuilderInput,
	) (map[commonmodels.ObjectID]string, error)
}
