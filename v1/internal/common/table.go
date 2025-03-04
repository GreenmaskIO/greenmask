package common

type Table struct {
	Schema     string
	Name       string
	Columns    []*Column
	Size       int64
	PrimaryKey []string
}

type Column struct {
	Idx      int
	Name     string
	TypeName string
	// TypeOid - can be either a real oid like in postgresql or virtual oid that exists only in
	// the driver implementation
	TypeOid           uint32
	CanonicalTypeName string
	NotNull           bool
	Size              int
}
