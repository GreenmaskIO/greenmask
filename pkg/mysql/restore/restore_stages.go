package restore

import (
	"context"
	"fmt"

	_ "github.com/go-sql-driver/mysql"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/restore/metadatareader"
	restorepipeline "github.com/greenmaskio/greenmask/pkg/common/restore/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/restore/processor"
	restorestorage "github.com/greenmaskio/greenmask/pkg/common/restore/storage"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/connconfig"
	mysqlrestorefactory "github.com/greenmaskio/greenmask/pkg/mysql/restore/factory"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/planbuilder"
)

// --- ConnectionConfigurerBuilder ---

type RestoreConnectionConfigurerBuilder struct{}

func (b *RestoreConnectionConfigurerBuilder) Build(cfg any) (core.ConnectionConfigurer, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	poolSize := c.Restore.Options.Jobs
	if poolSize <= 0 {
		poolSize = 1
	}
	return &connconfig.RestoreConnectionConfig{
		Common:             c.Restore.Options,
		MySQL:              c.Restore.MysqlConfig,
		ConnectionPoolSize: poolSize,
	}, nil
}

// --- RestoreStorageProvisioner ---

type MysqlRestoreStorageProvisioner struct{}

func (p *MysqlRestoreStorageProvisioner) Provision(
	ctx context.Context,
	cfg any,
	dumpID core.DumpID,
) (core.Storager, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return nil, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	baseSt, err := utils.GetStorage(ctx, &c)
	if err != nil {
		return nil, fmt.Errorf("get base storage: %w", err)
	}
	st, err := restorestorage.GetStorageByDumpID(ctx, baseSt, dumpID, c.Common.HeartbeatInterval)
	if err != nil {
		return nil, fmt.Errorf("resolve dumpID: %w", err)
	}
	return st, nil
}

// --- RestoreInstructionBuilder ---

type MysqlRestoreInstructionBuilder struct{}

func (b *MysqlRestoreInstructionBuilder) Build(_ context.Context, cfg any) (core.RestoreInstruction, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return core.RestoreInstruction{}, fmt.Errorf("unexpected config type %T, want config.Config", cfg)
	}
	return core.RestoreInstruction{
		Jobs:           c.Restore.Options.Jobs,
		DataOnly:       c.Restore.Options.DataOnly,
		SchemaOnly:     c.Restore.Options.SchemaOnly,
		RestoreInOrder: c.Restore.Options.RestoreInOrder,
		Section:        c.Restore.Options.Section,
		Scripts:        c.Restore.Scripts,
	}, nil
}

// --- Wiring ---

// NewRestoreStages returns a RestoreStages wired with all MySQL-specific
// stage implementations using the V2 registry-based restore processor.
func NewRestoreStages(cmd utils.CmdProducer) (restorepipeline.RestoreStages, error) {
	objectReg, err := mysqlrestorefactory.NewObjectRestoreRegistry()
	if err != nil {
		return restorepipeline.RestoreStages{}, fmt.Errorf("build mysql object restore registry: %w", err)
	}
	schemaReg, err := mysqlrestorefactory.NewSchemaRestoreRegistry(cmd)
	if err != nil {
		return restorepipeline.RestoreStages{}, fmt.Errorf("build mysql schema restore registry: %w", err)
	}
	restoreProcessor, err := processor.NewDefaultRestoreProcessorV2(objectReg, schemaReg, core.DBMSEngineMySQL)
	if err != nil {
		return restorepipeline.RestoreStages{}, fmt.Errorf("build restore processor v2: %w", err)
	}
	return restorepipeline.RestoreStages{
		ConnectionConfigurerBuilder: &RestoreConnectionConfigurerBuilder{},
		DatabaseSessionBuilder:      &RestoreSessionBuilder{},
		RestoreStorageProvisioner:   &MysqlRestoreStorageProvisioner{},
		RestoreMetadataReader:       metadatareader.New(),
		RestoreInstructionBuilder:   &MysqlRestoreInstructionBuilder{},
		RestorePlanBuilder:          planbuilder.New(),
		RestoreProcessor:            restoreProcessor,
	}, nil
}

// NewRestorePipeline returns a RestorePipeline wired for MySQL.
func NewRestorePipeline(cmd utils.CmdProducer) (*restorepipeline.RestorePipeline, error) {
	stages, err := NewRestoreStages(cmd)
	if err != nil {
		return nil, err
	}
	return restorepipeline.NewRestorePipeline(stages, core.DBMSEngineMySQL), nil
}
