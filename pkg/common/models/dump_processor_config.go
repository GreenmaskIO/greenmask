package models

// DumpProcessorConfig holds runtime execution options for DumpProcessor.
// Populated by applying DumpProcessorOption functions before execution starts.
type DumpProcessorConfig struct {
	// Options will be defined as execution requirements become known.
}

type DumpProcessorOption func(*DumpProcessorConfig)
