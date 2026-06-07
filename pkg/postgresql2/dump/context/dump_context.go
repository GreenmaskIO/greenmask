package context

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	dumpcontext "github.com/greenmaskio/greenmask/pkg/common/dump/context"
	transformerutils "github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
)

type DumpContextBuilder struct {
	transformerRegistry *transformerutils.TransformerRegistry
	newDriverFunc       dumpcontext.NewTableDriverFunc
	tableConfigs        []core.TableConfig
}

func NewDumpContextBuilder(
	tableConfigs []core.TableConfig,
	transformerRegistry *transformerutils.TransformerRegistry,
) *DumpContextBuilder {
	return &DumpContextBuilder{
		transformerRegistry: transformerRegistry,
		tableConfigs:        tableConfigs,
		//registry:            registry,
		//filter:              filter,
		//tableDriverFactory:  tableDriverFactory,
		//queryBuilder:        queryBuilder,
	}
}

func (b *DumpContextBuilder) buildTableObject(
	ctx context.Context,
	obj core.Object,
) (core.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildTables(
	ctx context.Context,
	result core.IntrospectionResult,
) ([]core.ObjectDumpSpec, error) {
	var tableObjects []core.ObjectDumpSpec
	for _, obj := range result.KindsMap[core.ObjectKindPostgresTable] {
		dumpContext, err := b.buildTableObject(ctx, obj)
		if err != nil {
			return nil, fmt.Errorf("build table dump context: %v", err)
		}
		tableObjects = append(tableObjects, dumpContext)
	}
	return tableObjects, nil
}

func (b *DumpContextBuilder) buildSequenceObject(
	ctx context.Context,
	obj core.Object,
) (core.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildSequences(
	ctx context.Context,
	result core.IntrospectionResult,
) ([]core.ObjectDumpSpec, error) {
	var tableObjects []core.ObjectDumpSpec
	for _, obj := range result.KindsMap[core.ObjectKindPostgresSequence] {
		dumpContext, err := b.buildSequenceObject(ctx, obj)
		if err != nil {
			return nil, fmt.Errorf("build sequence dump context: %v", err)
		}
		tableObjects = append(tableObjects, dumpContext)
	}
	return tableObjects, nil
}

func (b *DumpContextBuilder) buildBlobObject(
	ctx context.Context,
	obj core.Object,
) (core.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildBlobs(
	ctx context.Context,
	result core.IntrospectionResult,
) ([]core.ObjectDumpSpec, error) {
	var tableObjects []core.ObjectDumpSpec
	for _, obj := range result.KindsMap[core.ObjectKindPostgresBlobs] {
		dumpContext, err := b.buildBlobObject(ctx, obj)
		if err != nil {
			return nil, fmt.Errorf("build blob dump context: %v", err)
		}
		tableObjects = append(tableObjects, dumpContext)
	}
	return tableObjects, nil
}

func (b *DumpContextBuilder) Build(
	ctx context.Context,
	result core.IntrospectionResult,
) (core.DumpContext, error) {
	var res []core.ObjectDumpSpec

	tablesDumpObjects, err := b.buildTables(ctx, result)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, tablesDumpObjects...)

	sequencesDumpObjects, err := b.buildSequences(ctx, result)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, sequencesDumpObjects...)

	blobsDumpObjects, err := b.buildBlobs(ctx, result)
	if err != nil {
		return core.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, blobsDumpObjects...)

	return core.DumpContext{
		DumpObjectSpecs: tablesDumpObjects,
	}, nil
}
