package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/greenmaskio/greenmask/v1/internal/common/datadump"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/storages"
)

const (
	defaultInitTimeout = 30 * time.Second
)

// Dump2 it's responsible for initialization and perform the whole
// dump procedure of mysql instance.
type Dump2 struct {
	dumpID commonmodels.DumpID
	st     storages.Storager
	vc     *validationcollector.Collector
}

func NewDump2(
	ctx context.Context,
	cfg *config.Config,
) (*Dump2, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultInitTimeout)
	defer cancel()

	st, err := datadump.GetStorage(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get dump storage: %w", err)
	}

	dumpID := commonmodels.NewDumpID()
	st = storages.SubStorageWithDumpID(st, dumpID)
	vc := validationcollector.NewCollectorWithMeta(
		commonmodels.MetaKeyParameterName, dumpID,
	)
	return &Dump2{
		st:     st,
		dumpID: dumpID,
		vc:     vc,
	}, nil
}

/*
It must:
  - Create Storgae with DumpID provided
  - Initialize validation Collector
  - Introspect schema
  - Generate subsets
  - Rewrite a config if some requirements are met (FK inharitance / partitioning)
  - Initialize TableRuntime (transformers, conditions, dump queries)
  - Run heartbeat worker
  - Generate schema dump
  - Generate data dump -> Receives (task producer) -> Produce tasks -> execute on the workerы
  - Generate metadata based on the config, collected tables and some additional data
  - Complete dump

How tasks producer should work?
- Dedicate producer for each type of data object (table, sequences, large objects, etc.)

For tables:
- Produce raw dumper if there is no transformations - need to store data only.
- Produce TransformationPipelineDumper - executes transformer one by one and valudates conditions.
*/
func (d *Dump2) Run(ctx context.Context) error {
	return nil
}
