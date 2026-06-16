package core

import "context"

type TransformerContexter interface {
	SetRecordForDynamicParameters(r Recorder)
	EvaluateWhen(r Recorder) (bool, error)
	Init(ctx context.Context) error
	// Transform applies the underlying transformer to the record.
	Transform(ctx context.Context, r Recorder) error
	// Done terminates the underlying transformer, releasing any runtime
	// resources it acquired during Init.
	Done(ctx context.Context) error
	GetAffectedColumns() map[int]string
	Describe() string
	// GetSnapshot builds a deterministic TransformationSnapshot from the
	// initialized runtime parameters (resolved defaults included). position is
	// the transformer's index within its table's transformer list.
	GetSnapshot(position int) (TransformationSnapshot, error)
}

type TableContexter interface {
	HasTransformer() bool
	GetAffectedColumns() []int
	EvaluateWhen(r Recorder) (bool, error)
	Init(ctx context.Context) error
	// GetSnapshot builds the engine-agnostic portion of an ObjectSnapshot
	// (attributes, subset query, condition, transformations) on demand from the
	// runtime context. Engine-specific fields (identity, key, need-schema-dump)
	// are overlaid by the caller.
	GetSnapshot() (ObjectSnapshot, error)
}
