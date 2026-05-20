package interfaces

import commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"

type DumpContextDiffer interface {
	Diff() commonmodels.DumpContextSnapshot
}
