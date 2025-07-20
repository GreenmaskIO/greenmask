package cmdrun

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	commonutils "github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/config"
	"github.com/greenmaskio/greenmask/v1/internal/mysql"
)

const (
	engineNameMySQL    = "mysql"
	engineNamePostgres = "postgresql"
)

var (
	errUnsupportedEngine  = errors.New("unsupported DBMS engine")
	errEngineNotSpecified = errors.New("dbms engine is not specified")
)

// RunDump - runs dump for the specified DBMS engine.
func RunDump(cfg *config.Config) error {
	ctx := context.Background()
	st, err := commonutils.GetStorage(ctx, cfg)
	if err != nil {
		return fmt.Errorf("get storage: %w", err)
	}
	if err := commonutils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	if cfg.Engine == "" {
		return fmt.Errorf("specify dbma engine in \"engine\" key in the config: %w", errEngineNotSpecified)
	}
	ctx = log.Ctx(ctx).With().Str(commonmodels.MetaKeyEngine, cfg.Engine).Logger().WithContext(ctx)
	switch cfg.Engine {
	case engineNameMySQL:
		if err := mysql.RunDump(ctx, cfg, st); err != nil {
			return fmt.Errorf("mysql engine dump: %w", err)
		}
	case engineNamePostgres:
		panic("not implemented yet")
	default:
		return fmt.Errorf("engine \"%s\" is not supported: %w", cfg.Engine, errUnsupportedEngine)
	}
	return nil
}
