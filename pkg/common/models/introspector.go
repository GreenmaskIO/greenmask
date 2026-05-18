package models

type IntrospectionResult struct {
	Engine              DBMSEngine
	Version             DBMSVersion
	KindsMap            map[ObjectKind][]Object
	DumpRelatedSettings DumpScope
}
