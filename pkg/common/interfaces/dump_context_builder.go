package interfaces

import (
	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type ExplicitDumpContextBuilder interface {
	BuildDumpContext(in models.ExplicitDumpContextInput) (models.DumpContext, error)
}

type DerivedDumpContextBuilder interface {
	BuildDumpContext(in models.DerivedDumpContextInput, explicit models.DumpContext) (models.DumpContext, error)
}
