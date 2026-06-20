package dump

import (
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/dump/pipeline"
)

var errNotImplemented = fmt.Errorf("postgresql2 dump pipeline: not yet implemented")

// NewDumpPipeline is a placeholder until the PostgreSQL v2 dump stages are
// implemented. Returns errNotImplemented so the factory propagates a clean
// error rather than panicking.
func NewDumpPipeline() (*pipeline.DumpPipeline, error) {
	return nil, errNotImplemented
}
