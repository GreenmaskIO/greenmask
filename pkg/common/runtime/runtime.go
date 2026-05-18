package runtime

import (
	commonininterfaces "github.com/greenmaskio/greenmask/pkg/common/interfaces"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/taskproducer"
)

type Runtime struct {
	EngineName   commonmodels.DBMSEngine
	Capabilities Capabilities
	Introspector commonininterfaces.IntrospectorV2
	Planner      taskproducer.DumpObjectPoducer
	Executors    TaskExecutorRegistry
}

type Capabilities struct {
	SupportsSubset          bool
	SupportsParallelDump    bool
	SupportsTransactional   bool
	SupportsSchemaDiff      bool
	SupportsConsistentRead  bool
	SupportsIncrementalDump bool
}

type Object struct {
	ID   commonmodels.ObjectID
	Kind commonmodels.ObjectKind
	Name string
	// Engine specific payload.
	// e.g. *postgres.Table, *oracle.Package, *mongo.Collection
	Payload any
}
