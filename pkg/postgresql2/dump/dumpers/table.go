package dumpers

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	dumpcontext "github.com/greenmaskio/greenmask/pkg/common/dump/context"
	"github.com/greenmaskio/greenmask/pkg/common/dump/dumpers"
	"github.com/greenmaskio/greenmask/pkg/common/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/rawrecord"
	"github.com/greenmaskio/greenmask/pkg/common/record"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	"github.com/greenmaskio/greenmask/pkg/postgresql/dump/dumpers2/table"
	kinds "github.com/greenmaskio/greenmask/pkg/postgresql2/kinds"
)

type TableDumpObjectFactory struct {
	registry *registry.TransformerRegistry
	queries  map[core.ObjectID]string
}

func NewTableDumpObjectFactory(
	registry *registry.TransformerRegistry,
	queries map[core.ObjectID]string,
) *TableDumpObjectFactory {
	return &TableDumpObjectFactory{
		registry: registry,
		queries:  queries,
	}
}

func (f *TableDumpObjectFactory) Kind() core.ObjectKind {
	return kinds.ObjectKindTable
}

func (f *TableDumpObjectFactory) NewDumpObject(
	spec core.ObjectDumpSpec,
) (core.ObjectDumper, error) {
	tableContext, ok := spec.Payload.(dumpcontext.TableDumpContextPayload)
	if !ok {
		return nil, fmt.Errorf("expected context.TableDumpContextPayload, got %T", spec.Payload)
	}
	if spec.Kind != kinds.ObjectKindTable {
		return nil, fmt.Errorf("expected context.TableDumpObjectPayload, got %s", spec.Kind)
	}
	if tableContext.HasTransformer() {
		return f.initTableDumperWithPipeline(spec, tableContext)
	}
	return f.initTableDumperWithRaw(spec, tableContext)
}

func (f *TableDumpObjectFactory) initTableDumperWithPipeline(
	spec core.ObjectDumpSpec,
	tableContext dumpcontext.TableDumpContextPayload,
) (core.ObjectDumper, error) {
	tr := table.NewReader()
	tw := table.NewWriter()
	rawRecord := rawrecord.NewRawRecord(len(tableContext.Table.Columns), core.NullValueSeq)
	r := record.NewRecord(rawRecord, tableContext.TableDriver)
	p := pipeline.NewTransformationPipeline(&tableContext)

	dumper, err := dumpers.NewTableDumper(spec.TaskID, tr, tw, r, p, tableContext.Table, opts...)
	if err != nil {
		return nil, fmt.Errorf("create table dumper: %w", err)
	}
	return dumper, nil
}

func (f *TableDumpObjectFactory) initTableDumperWithRaw(
	spec core.ObjectDumpSpec,
	tableContext dumpcontext.TableDumpContextPayload,
) (core.ObjectDumper, error) {
	tr := table.NewReader()
	tw := table.NewWriter()
	return dumpers.NewTableRawDumper(spec.TaskID, tr, tw, tableContext.Table), nil
}
