package cmdrun

import (
	"context"
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/v1/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/v1/pkg/common/models"
	"github.com/greenmaskio/greenmask/v1/pkg/common/transformers/registry"
	commonutils "github.com/greenmaskio/greenmask/v1/pkg/common/utils"
	validate2 "github.com/greenmaskio/greenmask/v1/pkg/common/validate"
	"github.com/greenmaskio/greenmask/v1/pkg/common/validationcollector"
	config2 "github.com/greenmaskio/greenmask/v1/pkg/config"
	mysqldump "github.com/greenmaskio/greenmask/v1/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/v1/pkg/storages/validate"
)

const (
	JsonFormat string = "json"
	TextFormat string = "text"
)

const (
	VerticalTableFormat   = "vertical"
	HorizontalTableFormat = "horizontal"
)

const (
	nonZeroExitCode = 1
	zeroExitCode    = 0
)

func getMySQLDumpFilter(cfg config2.Validate) (mysqldump.Option, error) {
	filters := make([]models.TableFilter, 0, len(cfg.Tables))
	for i := range cfg.Tables {
		tf, err := models.NewTableFilterItemFromString(cfg.Tables[i])
		if err != nil {
			return nil, fmt.Errorf("create table filter from string %q: %w", cfg.Tables[i], err)
		}
		filters = append(filters, tf)
	}
	return mysqldump.WithFilter(models.TaskProducerFilter{Tables: filters}), nil
}

func getMySQLDumpOpts(cfg *config2.Config) ([]mysqldump.Option, error) {
	var opts []mysqldump.Option
	if cfg.Validate.Diff {
		opts = append(opts, mysqldump.WithSaveOriginal(true))
	}
	if cfg.Validate.RowsLimit > 0 {
		opts = append(opts, mysqldump.WithRowsLimit(int64(cfg.Validate.RowsLimit)))
	}
	opts = append(opts, mysqldump.WithDataOnly(), mysqldump.WithTransformedTablesOnly())
	if len(cfg.Validate.Tables) > 0 {
		filterOpt, err := getMySQLDumpFilter(cfg.Validate)
		if err != nil {
			return nil, fmt.Errorf("get mysql dump filter: %w", err)
		}
		opts = append(opts, filterOpt)
	}
	opts = append(opts, mysqldump.WithCompression(false, false))
	return opts, nil
}

func printValidateWarning(ctx context.Context, cfg *config2.Config) error {
	err := commonutils.PrintValidationWarnings(ctx, cfg.Validate.ResolvedWarnings, cfg.Validate.Warnings)
	if err != nil {
		return fmt.Errorf("print validation warnings: %w", err)
	}
	vc := validationcollector.FromContext(ctx)
	if vc.IsFatal() {
		return models.ErrFatalValidationError
	}
	return nil
}

func runMySQLValidate(ctx context.Context, st interfaces.Storager, cfg *config2.Config) (int, error) {
	opts, err := getMySQLDumpOpts(cfg)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("get mysql dump options: %w", err)
	}
	dump, err := mysqldump.NewDump(cfg, registry.DefaultTransformerRegistry, st, opts...)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("init dump process: %w", err)
	}
	if err := dump.Run(ctx); err != nil {
		if printErr := printValidateWarning(ctx, cfg); printErr != nil {
			if errors.Is(err, models.ErrFatalValidationError) {
				return nonZeroExitCode, nil
			}
			return nonZeroExitCode, errors.Join(err, printErr)
		}
		return nonZeroExitCode, fmt.Errorf("run mysql dump for validation: %w", err)
	}
	if err := printValidateWarning(ctx, cfg); err != nil {
		return nonZeroExitCode, err
	}
	if cfg.Validate.Data {
		if err := validate2.PrintData(ctx, st, cfg); err != nil {
			return nonZeroExitCode, fmt.Errorf("print data: %w", err)
		}
	}
	return zeroExitCode, nil
}

func RunValidate(cfg *config2.Config) (int, error) {
	ctx := context.Background()
	ctx = setupContext(ctx, cfg)
	if err := setupInfrastructure(cfg); err != nil {
		return nonZeroExitCode, fmt.Errorf("setup infrastructure: %w", err)
	}
	st := validate.New("")
	switch cfg.Engine {
	case engineNameMySQL:
		return runMySQLValidate(ctx, st, cfg)
	case engineNamePostgres:
		panic("not implemented yet")
	default:
		return nonZeroExitCode, fmt.Errorf("engine \"%s\" is not supported: %w", cfg.Engine, errUnsupportedEngine)
	}
	return zeroExitCode, nil
}
