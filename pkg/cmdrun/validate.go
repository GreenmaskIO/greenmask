package cmdrun

import (
	"context"
	"errors"
	"fmt"

	"github.com/greenmaskio/greenmask/pkg/common/interfaces"
	"github.com/greenmaskio/greenmask/pkg/common/models"
	"github.com/greenmaskio/greenmask/pkg/common/transformers/registry"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	validate2 "github.com/greenmaskio/greenmask/pkg/common/validate"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	config2 "github.com/greenmaskio/greenmask/pkg/config"
	mysqldump "github.com/greenmaskio/greenmask/pkg/mysql/cmdrun/dump"
	"github.com/greenmaskio/greenmask/pkg/storages/validate"
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

func PrintValidateWarning(ctx context.Context, cfg *config2.Config) error {
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

func RunMySQLValidate(ctx context.Context, st interfaces.Storager, cfg *config2.Config) (int, error) {
	opts, err := mysqldump.GetMySQLDumpOptsWithValidate(cfg)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("get mysql dump options: %w", err)
	}
	dump, err := mysqldump.NewDump(cfg, registry.DefaultTransformerRegistry, st, opts...)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("init dump process: %w", err)
	}
	if err := dump.Run(ctx); err != nil {
		if printErr := PrintValidateWarning(ctx, cfg); printErr != nil {
			if errors.Is(err, models.ErrFatalValidationError) {
				return nonZeroExitCode, nil
			}
			return nonZeroExitCode, errors.Join(err, printErr)
		}
		return nonZeroExitCode, fmt.Errorf("run mysql dump for validation: %w", err)
	}
	if err := PrintValidateWarning(ctx, cfg); err != nil {
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
	ctx = SetupContext(ctx, cfg)
	if err := SetupInfrastructure(cfg); err != nil {
		return nonZeroExitCode, fmt.Errorf("setup infrastructure: %w", err)
	}
	st := validate.New("")
	switch cfg.Engine {
	case engineNameMySQL:
		return RunMySQLValidate(ctx, st, cfg)
	case engineNamePostgres:
		panic("not implemented yet")
	default:
		return nonZeroExitCode, fmt.Errorf("engine \"%s\" is not supported: %w", cfg.Engine, errUnsupportedEngine)
	}
}
