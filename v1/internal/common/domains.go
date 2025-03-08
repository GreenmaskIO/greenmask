package common

type Reference struct {
	Schema string
	Name   string
	// ReferencedKeys - list of foreign keys of current table
	ReferencedKeys []string
	IsNullable     bool
}
