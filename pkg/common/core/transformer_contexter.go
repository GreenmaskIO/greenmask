package core

import "context"

type TransformerContexter interface {
	SetRecordForDynamicParameters(r Recorder)
	EvaluateWhen(r Recorder) (bool, error)
	Init(ctx context.Context) error
	GetAffectedColumns() map[int]string
	Describe() string
}

type TableContexter interface {
	HasTransformer() bool
	GetAffectedColumns() []int
	EvaluateWhen(r Recorder) (bool, error)
	Init(ctx context.Context) error
}
