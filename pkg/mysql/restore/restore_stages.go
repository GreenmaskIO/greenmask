package restore

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/rs/zerolog/log"

	commonconfig "github.com/greenmaskio/greenmask/pkg/common/config"
	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/restore/metadatareader"
	restorepipeline "github.com/greenmaskio/greenmask/pkg/common/restore/pipeline"
	"github.com/greenmaskio/greenmask/pkg/common/restore/processor"
	"github.com/greenmaskio/greenmask/pkg/common/restore/script"
	restorestorage "github.com/greenmaskio/greenmask/pkg/common/restore/storage"
	"github.com/greenmaskio/greenmask/pkg/common/restore/taskmapper"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/schema"
	"github.com/greenmaskio/greenmask/pkg/mysql/restore/taskproducer"
)

// RestoreConnectionConfig is the MySQL-specific ConnectionConfigurer for the
// restore pipeline. It carries the target-DB connection parameters from the
// restore config section (cfg.Restore.*), not the dump source.
type RestoreConnectionConfig struct {
	Common             config.CommonRestoreOptions
	MySQL              config.MysqlRestoreConfig
	ConnectionPoolSize int
}

func (c *RestoreConnectionConfig) ConnectionConfig() any { return c }

// SchemaRestoreParams returns the mysql CLI connection/auth flags plus any
// user-specified vendor options. It satisfies schema.options.
func (c *RestoreConnectionConfig) SchemaRestoreParams(ssl commonconfig.SSLOpts) ([]string, error) {
	return c.MySQL.SchemaRestoreParams(ssl)
}

// Env returns the process environment for mysql CLI invocations.
// It satisfies schema.options.
func (c *RestoreConnectionConfig) Env() ([]string, error) {
	return c.MySQL.Env()
}

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
	return &RestoreConnectionConfig{
		Common:             c.Restore.Options,
		MySQL:              c.Restore.MysqlConfig,
		ConnectionPoolSize: poolSize,
	}, nil
}

// --- DatabaseSession / DatabaseSessionBuilder ---

// MysqlRestoreSession wraps a *sql.DB as core.DatabaseSession.
// For restore there is no pool or snapshot — a single connection suffices.
type MysqlRestoreSession struct {
	db *sql.DB
}

func (s *MysqlRestoreSession) Close(_ context.Context) error { return s.db.Close() }

func (s *MysqlRestoreSession) RunWithOperationalDB(
	ctx context.Context,
	fn func(ctx context.Context, db core.DB) error,
) error {
	return fn(ctx, s.db)
}

func (s *MysqlRestoreSession) RunWithEngineResource(_ context.Context, _ func(context.Context, any) error) error {
	return core.ErrEngineResourceNotSupported
}

type MysqlRestoreSessionBuilder struct{}

func (b *MysqlRestoreSessionBuilder) Open(ctx context.Context, cfg core.ConnectionConfigurer) (core.DatabaseSession, error) {
	c, ok := cfg.ConnectionConfig().(*RestoreConnectionConfig)
	if !ok {
		return nil, fmt.Errorf("unexpected connection config type %T, want *RestoreConnectionConfig", cfg.ConnectionConfig())
	}
	connCfg, err := c.MySQL.ConnectionConfig(c.Common.SSL)
	if err != nil {
		return nil, fmt.Errorf("build mysql connection config: %w", err)
	}
	uri, err := connCfg.URI()
	if err != nil {
		return nil, fmt.Errorf("build connection URI: %w", err)
	}
	db, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, fmt.Errorf("open mysql connection: %w", err)
	}
	return &MysqlRestoreSession{db: db}, nil
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

// --- RestoreProcessor ---

// MysqlRestoreProcessor implements core.RestoreProcessor by orchestrating the
// existing MySQL restore components: taskproducer, schema.Restorer, and
// DefaultRestoreProcessor. It contains the logic previously in Restore.Run().
type MysqlRestoreProcessor struct {
	cmd utils.CmdProducer
}

func NewMysqlRestoreProcessor(cmd utils.CmdProducer) *MysqlRestoreProcessor {
	return &MysqlRestoreProcessor{cmd: cmd}
}

func (p *MysqlRestoreProcessor) Run(ctx context.Context, input core.RestoreRunInput) error {
	if err := input.Validate(); err != nil {
		return fmt.Errorf("validate restore run input: %w", err)
	}
	c, ok := input.Conn.ConnectionConfig().(*RestoreConnectionConfig)
	if !ok {
		return fmt.Errorf("unexpected connection config type %T, want *RestoreConnectionConfig", input.Conn.ConnectionConfig())
	}
	mysqlSession, ok := input.Session.(*MysqlRestoreSession)
	if !ok {
		return fmt.Errorf("unexpected session type %T, want *MysqlRestoreSession", input.Session)
	}
	meta := input.Meta
	st := input.St
	instr := input.Instruction

	remap, err := remapDB(c.Common, meta)
	if err != nil {
		return fmt.Errorf("remap databases: %w", err)
	}

	connCfg, err := c.MySQL.ConnectionConfig(c.Common.SSL)
	if err != nil {
		return fmt.Errorf("build mysql connection config: %w", err)
	}

	opts := taskproducer.RestoreOptions{
		PrintWarnings:           c.MySQL.PrintWarnings,
		MaxFetchWarnings:        c.MySQL.MaxFetchWarnings,
		DisableForeignKeyChecks: c.MySQL.DisableForeignKeyChecks,
		DisableUniqueChecks:     c.MySQL.DisableUniqueChecks,
		InsertIgnore:            c.MySQL.InsertIgnore,
		InsertReplace:           c.MySQL.InsertReplace,
		MaxInsertStatementSize:  c.MySQL.MaxInsertStatementSize,
		DatabaseRemap:           remap,
	}

	hasTopologicalOrder := meta.DataDump != nil &&
		meta.DataDump.DumpStat.RestorationContext.HasTopologicalOrder

	var tp core.RestoreTaskProducer
	if instr.RestoreInOrder && hasTopologicalOrder {
		log.Ctx(ctx).Info().Msg("restoring tables in topological order")
		tp = taskproducer.NewWithOrder(meta, st, connCfg, opts, taskmapper.NewTaskResolver())
	} else {
		if instr.RestoreInOrder && !hasTopologicalOrder {
			log.Ctx(ctx).Warn().Msg("restore-in-order requested but schema has FK cycles; falling back to unordered restore")
		}
		tp = taskproducer.New(meta, st, connCfg, opts)
	}

	schemaOpts := []schema.Option{}
	if c.Common.CreateDatabase && len(meta.Databases) > 0 {
		schemaOpts = append(schemaOpts, schema.WithCreateDatabase(mysqlSession.db, meta.Databases))
	}
	if c.Common.IfNotExists {
		schemaOpts = append(schemaOpts, schema.WithIfNotExists())
	}
	if len(remap) > 0 {
		schemaOpts = append(schemaOpts, schema.WithDatabaseRemap(remap))
	}
	sr := schema.NewRestorer(st, c, c.Common.SSL, p.cmd, meta.SchemaDump, schemaOpts...)

	var txExecBuilder script.TxExecBuilder
	if len(instr.Scripts) > 0 {
		txExecBuilder = func(ctx context.Context) (script.TxExec, func(), error) {
			uri, err := connCfg.URI()
			if err != nil {
				return nil, func() {}, fmt.Errorf("build script exec: get URI: %w", err)
			}
			db, err := sql.Open("mysql", uri)
			if err != nil {
				return nil, func() {}, fmt.Errorf("build script exec: open connection: %w", err)
			}
			exec := script.TxExec(func(ctx context.Context, q string) error {
				_, err := db.ExecContext(ctx, q)
				return err
			})
			return exec, func() {
				if err := db.Close(); err != nil {
					log.Ctx(ctx).Warn().Err(err).Msg("close script db connection")
				}
			}, nil
		}
	}

	if err := processor.NewDefaultRestoreProcessor(ctx, tp, sr, processor.Config{
		Jobs:           instr.Jobs,
		RestoreInOrder: instr.RestoreInOrder,
		DataOnly:       instr.DataOnly,
		SchemaOnly:     instr.SchemaOnly,
		Section:        instr.Section,
	}, instr.Scripts, txExecBuilder).Run(ctx); err != nil {
		return fmt.Errorf("run restore processor: %w", err)
	}
	return nil
}

// remapDB validates and returns the database name remap map from config.
func remapDB(opts config.CommonRestoreOptions, meta core.Metadata) (map[string]string, error) {
	remap := opts.RemapDatabase
	if len(remap) == 0 {
		return nil, nil
	}
	mode := opts.DatabaseReplaceMode
	if mode == "" {
		mode = core.DatabaseReplaceModeStrict
	}
	switch mode {
	case core.DatabaseReplaceModeStrict:
		for _, db := range meta.Databases {
			if _, ok := remap[db]; !ok {
				return nil, fmt.Errorf("database-replace-mode=strict: database %q has no entry in remap-database", db)
			}
		}
	case core.DatabaseReplaceModeRelaxed:
	default:
		return nil, fmt.Errorf("unknown database-replace-mode %q", mode)
	}
	return remap, nil
}

// --- Wiring ---

// NewRestoreStages returns a RestoreStages wired with all MySQL-specific
// stage implementations.
func NewRestoreStages() restorepipeline.RestoreStages {
	return restorepipeline.RestoreStages{
		ConnectionConfigurerBuilder: &RestoreConnectionConfigurerBuilder{},
		DatabaseSessionBuilder:      &MysqlRestoreSessionBuilder{},
		RestoreStorageProvisioner:   &MysqlRestoreStorageProvisioner{},
		RestoreMetadataReader:       metadatareader.New(),
		RestoreInstructionBuilder:   &MysqlRestoreInstructionBuilder{},
		RestoreProcessor:            NewMysqlRestoreProcessor(utils.NewDefaultCmdProducer()),
	}
}

// NewRestorePipeline returns a RestorePipeline wired for MySQL.
func NewRestorePipeline() *restorepipeline.RestorePipeline {
	return restorepipeline.NewRestorePipeline(NewRestoreStages(), core.DBMSEngineMySQL)
}
