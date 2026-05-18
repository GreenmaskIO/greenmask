package interfaces

import (
	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpFactory[Kind comparable, Spec any, Dumper any] interface {
	Kind() Kind
	New(spec Spec) (Dumper, error)
}

type ObjectDumpFactory = DumpFactory[models.ObjectKind, models.ObjectDumpSpec, ObjectDumper]
type SchemaDumpFactory = DumpFactory[models.SchemaDumpKind, models.SchemaDumpSpec, SchemaDumper]
