// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
)

var errEngineNotSpecified = errors.New("dbms engine is not specified")

func SetupContext(ctx context.Context, cfg *config.Config) context.Context {
	ctx = log.Ctx(ctx).With().Str(core.MetaKeyEngine, string(cfg.Engine)).Logger().WithContext(ctx)
	vc := validationcollector.NewCollectorWithMeta(core.MetaKeyEngine, cfg.Engine)
	ctx = validationcollector.WithCollector(ctx, vc)
	return ctx
}

func SetupInfrastructure(cfg *config.Config) error {
	if err := utils.SetDefaultContextLogger(cfg.Log.Level, cfg.Log.Format); err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	if cfg.Engine == "" {
		return fmt.Errorf("specify dbms engine in \"engine\" key in the config: %w", errEngineNotSpecified)
	}
	if err := cfg.Engine.Validate(); err != nil {
		return fmt.Errorf("invalid engine: %w", err)
	}
	return nil
}

type OutputFormat string

const (
	OutputFormatJSON OutputFormat = "json"
	OutputFormatText OutputFormat = "text"
)

func (of OutputFormat) Validate() error {
	switch of {
	case OutputFormatJSON, OutputFormatText:
		return nil
	default:
		return fmt.Errorf("format '%s': %w", of, core.ErrValueValidationFailed)
	}
}
