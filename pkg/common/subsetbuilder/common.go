package subsetbuilder

import (
	"fmt"

	"github.com/huandu/go-sqlbuilder"
)

// Dialect - represents the SQL dialect used for building queries.
type Dialect int

const (
	DialectPostgres = Dialect(sqlbuilder.PostgreSQL)
	DialectMySQL    = Dialect(sqlbuilder.MySQL)
)

func (d Dialect) String() string {
	switch d {
	case DialectPostgres:
		return "postgres"
	case DialectMySQL:
		return "mysql"
	default:
		return fmt.Sprintf("unknown dialect %d", d)
	}
}
