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
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/rs/zerolog/log"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/schemadiff"
	commonutils "github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/common/validate"
	"github.com/greenmaskio/greenmask/pkg/common/validationcollector"
	"github.com/greenmaskio/greenmask/pkg/config"
	"github.com/greenmaskio/greenmask/pkg/engines"
	validatest "github.com/greenmaskio/greenmask/pkg/storages/validate"
)

const (
	VerticalTableFormat = "vertical"
)

const (
	nonZeroExitCode = 1
	zeroExitCode    = 0
)

// Validate runs the validation pipeline. Fatal validation warnings are
// returned as core.ErrFatalValidationError so callers can distinguish
// them from infrastructure errors.
func (g *Cli) Validate(ctx context.Context) error {
	if err := g.initInfrastructure(); err != nil {
		return fmt.Errorf("setup infrastructure: %w", err)
	}
	ctx = SetupContext(ctx, g.cfg)
	st, err := g.storage(ctx)
	if err != nil {
		return err
	}
	exitCode, runErr := g.validateWithStorage(ctx, st)
	if runErr != nil {
		return runErr
	}
	if exitCode != zeroExitCode {
		return core.ErrFatalValidationError
	}
	return nil
}

// PrintValidateWarning prints collected validation warnings from ctx and
// returns an error when fatal warnings are present.
func PrintValidateWarning(ctx context.Context, cfg *config.Config) error {
	err := commonutils.PrintValidationWarnings(ctx, cfg.Validate.ResolvedWarnings, cfg.Validate.Warnings)
	if err != nil {
		return fmt.Errorf("print validation warnings: %w", err)
	}
	vc := validationcollector.FromContext(ctx)
	if vc.IsFatal() {
		return core.ErrFatalValidationError
	}
	return nil
}

// validateWithStorage runs the full validate pipeline using an already
// initialised context and storage. It is shared by RunValidate and
// Cli.Validate so that gm-backend can inject its own storage.
func (g *Cli) validateWithStorage(ctx context.Context, st core.Storager) (int, error) {
	validateSt := validatest.New("")
	validator, err := engines.NewValidator(g.cfg, validateSt)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("create validator: %w", err)
	}

	runErr := validator.Run(ctx)

	if printErr := PrintValidateWarning(ctx, g.cfg); printErr != nil {
		if errors.Is(runErr, core.ErrFatalValidationError) {
			return nonZeroExitCode, nil
		}
		if runErr != nil {
			return nonZeroExitCode, errors.Join(runErr, printErr)
		}
		return nonZeroExitCode, printErr
	}
	if runErr != nil {
		return nonZeroExitCode, fmt.Errorf("run validate: %w", runErr)
	}

	if g.cfg.Validate.Data {
		if err := validate.PrintData(ctx, validateSt, g.cfg); err != nil {
			return nonZeroExitCode, fmt.Errorf("print data: %w", err)
		}
	}

	if g.cfg.Validate.Schema {
		exitCode, err := g.diffWithPreviousSchema(ctx, st, validator.Introspection())
		if err != nil {
			return nonZeroExitCode, fmt.Errorf("diff with previous schema: %w", err)
		}
		return exitCode, nil
	}

	return zeroExitCode, nil
}

// diffWithPreviousSchema compares the current DB introspection against the
// most recent stored dump's schema. Returns nonZeroExitCode when differences
// are found, zeroExitCode when schemas match or no previous dump exists.
func (g *Cli) diffWithPreviousSchema(ctx context.Context, st core.Storager, current []core.Table) (int, error) {
	dumpId, err := getPreviousDumpId(ctx, st)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("get previous dump id: %w", err)
	}
	if dumpId == "" {
		return zeroExitCode, nil
	}

	md, err := getPreviousMetadata(ctx, st, dumpId)
	if err != nil {
		return nonZeroExitCode, fmt.Errorf("get previous metadata: %w", err)
	}

	diff := schemadiff.DatabaseSchema(md.Introspection).Diff(schemadiff.DatabaseSchema(current))
	if len(diff) == 0 {
		return zeroExitCode, nil
	}

	if err := g.printSchemaDiff(ctx, diff, dumpId); err != nil {
		return nonZeroExitCode, fmt.Errorf("print schema diff: %w", err)
	}
	return nonZeroExitCode, nil
}

// getPreviousDumpId scans storage for backup directories that contain a
// metadata file and returns the most recent dump ID (reverse-lexicographic,
// which is the most recent timestamp-based ID).
func getPreviousDumpId(ctx context.Context, st core.Storager) (string, error) {
	_, dirs, err := st.ListDir(ctx)
	if err != nil {
		return "", fmt.Errorf("list storage directory: %w", err)
	}
	var backupNames []string
	for _, dir := range dirs {
		exists, err := dir.Exists(ctx, MetadataJsonFileName)
		if err != nil {
			return "", fmt.Errorf("check metadata file existence: %w", err)
		}
		if exists {
			backupNames = append(backupNames, dir.Dirname())
		}
	}
	slices.SortFunc(backupNames, func(a, b string) int {
		if a > b {
			return -1
		}
		return 1
	})
	if len(backupNames) > 0 {
		return backupNames[0], nil
	}
	return "", nil
}

// getPreviousMetadata reads and decodes the metadata.json of a specific dump.
func getPreviousMetadata(ctx context.Context, st core.Storager, dumpId string) (core.Metadata, error) {
	sub := st.SubStorage(dumpId, true)
	f, err := sub.GetObject(ctx, MetadataJsonFileName)
	if err != nil {
		return core.Metadata{}, fmt.Errorf("open metadata file: %w", err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Ctx(ctx).Warn().Err(err).Msg("error closing metadata file")
		}
	}()
	var md core.Metadata
	if err = json.NewDecoder(f).Decode(&md); err != nil {
		return core.Metadata{}, fmt.Errorf("decode metadata file: %w", err)
	}
	return md, nil
}

// printSchemaDiff logs schema differences in the configured output format.
func (g *Cli) printSchemaDiff(ctx context.Context, diff []core.DiffNode, previousDumpId string) error {
	if validate.Format(g.cfg.Validate.Format) == validate.FormatNameJson {
		data, err := json.Marshal(diff)
		if err != nil {
			return fmt.Errorf("encode diff nodes: %w", err)
		}
		log.Ctx(ctx).Warn().
			Str("PreviousDumpId", previousDumpId).
			RawJSON("Diff", data).
			Str("Hint", "Check schema changes before making new dump").
			Msg("Database schema has been changed")
		return nil
	}
	log.Ctx(ctx).Warn().
		Str("PreviousDumpId", previousDumpId).
		Str("Hint", "Check schema changes before making new dump").
		Msg("Database schema has been changed")
	for _, node := range diff {
		log.Ctx(ctx).Warn().
			Str("Event", node.Event).
			Any("Signature", node.Signature).
			Msg(node.Msg)
	}
	return nil
}
