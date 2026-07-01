package schema

import "github.com/greenmaskio/greenmask/pkg/mysql/restore/opts"

// SchemaRestoreOpts is the set of parameters consumed by MysqlSchemaRestorer.
// Re-exported from the neutral opts package so callers that only import the
// schema package do not need an additional import.
type SchemaRestoreOpts = opts.SchemaRestoreOpts
