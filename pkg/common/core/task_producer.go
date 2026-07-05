package core

import (
	"context"
)

type DumpObjectProducer interface {
	Produce(ctx context.Context) (
		[]ObjectDumper,
		RestorationContext,
		error,
	)
}

type DumpObjectProducerV2 interface {
	Produce(ctx context.Context, result IntrospectionResult) ([]ObjectDumpSpec, error)
}
