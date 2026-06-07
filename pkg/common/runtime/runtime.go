package runtime

import (
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/mysql/dump/taskproducer"
)

type Runtime struct {
	EngineName   core.DBMSEngine
	Capabilities Capabilities
	Introspector core.IntrospectorV2
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
	ID   core.ObjectID
	Kind core.ObjectKind
	Name string
	// Engine specific payload.
	// e.g. *postgres.Table, *oracle.Package, *mongo.Collection
	Payload any
}
