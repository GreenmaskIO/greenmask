package table

import "github.com/greenmaskio/greenmask/pkg/mysql/restore/opts"

// TableRestoreOpts is the set of parameters consumed by InsertRestoreWriter.
// Re-exported from the neutral opts package so callers that only import the
// table package do not need an additional import.
type TableRestoreOpts = opts.TableRestoreOpts
