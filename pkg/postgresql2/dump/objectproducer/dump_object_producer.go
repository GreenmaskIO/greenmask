package objectproducer

import (
	"context"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/rs/zerolog/log"
)

type dumpContextBuilder interface {
	Build(ctx context.Context, result models.IntrospectionResult) (models.DumpContext, error)
}

type DumpPlanBuilder struct {
	dumpContextBuilder dumpContextBuilder
}

func NewDumpPlanBuilder(dumpContextBuilder dumpContextBuilder) *DumpPlanBuilder {
	return &DumpPlanBuilder{
		dumpContextBuilder: dumpContextBuilder,
	}
}

func (p *DumpPlanBuilder) buildRestorationContext(dependencyGraph any) (models.RestorationContext, error) {
	panic("implement me")
}

func (p *DumpPlanBuilder) Produce(
	ctx context.Context, result models.IntrospectionResult, dependencyGraph any,
) (models.DumpPlan, error) {
	dumpContext, err := p.dumpContextBuilder.Build(ctx, result)
	if err != nil {
		return models.DumpPlan{}, fmt.Errorf("build dump context: %w", err)
	}

	var dumpObjectSpecs []models.ObjectDumpSpec
	for _, dumpObjectSpec := range dumpContext.DumpObjectSpecs {
		if !dumpObjectSpec.NeedDumpData {
			log.Ctx(ctx).Debug().
				Str("ObjectName", dumpObjectSpec.Name).
				Msg("need dump data needed")
			continue
		}
		if dumpObjectSpec.TaskID == 0 {
			panic("ID not set")
		}
		dumpObjectSpecs = append(dumpObjectSpecs, dumpObjectSpec)
	}
	restorationCtx, err := p.buildRestorationContext(dependencyGraph)
	if err != nil {
		return models.DumpPlan{}, fmt.Errorf("build restoration context: %w", err)
	}
	return models.DumpPlan{
		DumpObjectSpecs:    dumpObjectSpecs,
		RestorationContext: restorationCtx,
	}, nil
}
