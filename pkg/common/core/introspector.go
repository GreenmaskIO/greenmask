package core

import "slices"

// IntrospectionResult is the complete, config-independent picture of the source
// database produced by the introspection stage. It deliberately carries no
// include/exclude filter settings: it is compared across runs by the schema
// drift validator and persisted into Metadata, so embedding user filters here
// would surface spurious "drift" and conflate "what the database looks like"
// with "what was asked to be dumped".
//
// The dump scope (DumpScope: allowed databases/schemas and include/exclude table
// and table-data lists used to drive vendor CLI tools such as mysqldump) is a
// derived artifact — config filters resolved against this introspection. It must
// be computed by a later context-building stage (e.g. the dump context builder)
// from config.Dump and this result, and carried on DumpContext/DumpPlan, not here.
type IntrospectionResult struct {
	Engine   DBMSEngine
	Version  DBMSVersion
	KindsMap map[ObjectKind][]Object
}

func (m *IntrospectionResult) GetKinds() []ObjectKind {
	kinds := make([]ObjectKind, 0, len(m.KindsMap))
	for kind := range m.KindsMap {
		kinds = append(kinds, kind)
	}
	slices.Sort(kinds)
	return kinds
}
