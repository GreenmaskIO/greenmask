package interfaces

import (
	"context"

	"github.com/greenmaskio/greenmask/pkg/common/models"
)

type DumpObjectProducer interface {
	Produce(ctx context.Context) (
		[]ObjectDumper,
		models.RestorationContext,
		error,
	)
}

type DumpObjectProducerV2 interface {
	Produce(ctx context.Context, result models.IntrospectionResult) ([]models.ObjectDumpSpec, error)
}
