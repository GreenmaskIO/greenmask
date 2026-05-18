package context

import (
	"context"
	"fmt"

	dumpcontext "github.com/greenmaskio/greenmask/pkg/common/dump/context"
	commonmodels "github.com/greenmaskio/greenmask/pkg/common/models"
	transformerutils "github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
)

type DumpContextBuilder struct {
	transformerRegistry *transformerutils.TransformerRegistry
	newDriverFunc       dumpcontext.NewTableDriverFunc
	tableConfigs        []commonmodels.TableConfig
}

func NewDumpContextBuilder(
	tableConfigs []commonmodels.TableConfig,
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
	obj commonmodels.Object,
) (commonmodels.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildTables(
	ctx context.Context,
	result commonmodels.IntrospectionResult,
) ([]commonmodels.ObjectDumpSpec, error) {
	var tableObjects []commonmodels.ObjectDumpSpec
	for _, obj := range result.KindsMap[commonmodels.ObjectKindPostgresTable] {
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
	obj commonmodels.Object,
) (commonmodels.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildSequences(
	ctx context.Context,
	result commonmodels.IntrospectionResult,
) ([]commonmodels.ObjectDumpSpec, error) {
	var tableObjects []commonmodels.ObjectDumpSpec
	for _, obj := range result.KindsMap[commonmodels.ObjectKindPostgresSequence] {
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
	obj commonmodels.Object,
) (commonmodels.ObjectDumpSpec, error) {
	panic("implement me")
}

func (b *DumpContextBuilder) buildBlobs(
	ctx context.Context,
	result commonmodels.IntrospectionResult,
) ([]commonmodels.ObjectDumpSpec, error) {
	var tableObjects []commonmodels.ObjectDumpSpec
	for _, obj := range result.KindsMap[commonmodels.ObjectKindPostgresBlobs] {
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
	result commonmodels.IntrospectionResult,
) (commonmodels.DumpContext, error) {
	var res []commonmodels.ObjectDumpSpec

	tablesDumpObjects, err := b.buildTables(ctx, result)
	if err != nil {
		return commonmodels.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, tablesDumpObjects...)

	sequencesDumpObjects, err := b.buildSequences(ctx, result)
	if err != nil {
		return commonmodels.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, sequencesDumpObjects...)

	blobsDumpObjects, err := b.buildBlobs(ctx, result)
	if err != nil {
		return commonmodels.DumpContext{}, fmt.Errorf("build tables: %v", err)
	}
	res = append(res, blobsDumpObjects...)

	return commonmodels.DumpContext{
		DumpObjectSpecs: tablesDumpObjects,
	}, nil
}
