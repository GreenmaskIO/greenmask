package objectproducer

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/rs/zerolog/log"
)

type dumpContextBuilder interface {
	Build(ctx context.Context, result core.IntrospectionResult) (core.DumpContext, error)
}

type DumpPlanBuilder struct {
	dumpContextBuilder dumpContextBuilder
}

func NewDumpPlanBuilder(dumpContextBuilder dumpContextBuilder) *DumpPlanBuilder {
	return &DumpPlanBuilder{
		dumpContextBuilder: dumpContextBuilder,
	}
}

func (p *DumpPlanBuilder) buildRestorationContext(dependencyGraph any) (core.RestorationContext, error) {
	panic("implement me")
}

func (p *DumpPlanBuilder) Produce(
	ctx context.Context, result core.IntrospectionResult, dependencyGraph any,
) (core.DumpPlan, error) {
	dumpContext, err := p.dumpContextBuilder.Build(ctx, result)
	if err != nil {
		return core.DumpPlan{}, fmt.Errorf("build dump context: %w", err)
	}

	var dumpObjectSpecs []core.ObjectDumpSpec
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
		return core.DumpPlan{}, fmt.Errorf("build restoration context: %w", err)
	}
	return core.DumpPlan{
		DumpObjectSpecs:    dumpObjectSpecs,
		RestorationContext: restorationCtx,
	}, nil
}
