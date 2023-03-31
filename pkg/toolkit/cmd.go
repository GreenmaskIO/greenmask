// Copyright 2023 Greenmask
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

package toolkit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"

	"github.com/greenmaskio/greenmask/internal/utils/logger"
)

type NewRowDriverFunc func() RowDriver

type Cmd struct {
	*cobra.Command
	definition      *Definition
	rawMeta         string
	logLevel        string
	logFormat       string
	meta            *Meta
	printDefinition bool
	validate        bool
	transform       bool
	params          map[string]*Parameter
}

func NewCmd(definition *Definition) *Cmd {

	if definition == nil {
		panic("definition cannot be nil")
	}

	if definition.Name == "" {
		panic("definition Name attribute is required")
	}

	if definition.New == nil {
		panic("definition New cannot be nil")
	}

	tc := &Cmd{
		definition: definition,
	}

	cmd := &cobra.Command{
		Use:   definition.Name,
		Short: definition.Description,
		Run:   tc.run,
	}
	tc.Command = cmd
	tc.setupDefaultCmd()

	return tc
}

func (c *Cmd) setupDefaultCmd() {

	c.PersistentFlags().BoolVar(&c.transform, "transform", false, "run transformation")
	c.PersistentFlags().BoolVar(&c.validate, "validate", false, "validate using provided meta")
	c.PersistentFlags().BoolVar(&c.printDefinition, "print-definition", false, "print transformer definition")
	c.PersistentFlags().StringVar(&c.rawMeta, "meta", "", "runtime metadata")
	c.MarkFlagsMutuallyExclusive("transform", "validate", "print-definition")
	c.PersistentFlags().StringVar(&c.logFormat, "log-format", "text", "logging format [text|json]")
	c.PersistentFlags().StringVar(&c.logLevel, "log-level", zerolog.LevelInfoValue,
		fmt.Sprintf(
			"logging level %s|%s|%s",
			zerolog.LevelDebugValue,
			zerolog.LevelInfoValue,
			zerolog.LevelWarnValue,
		),
	)
}

func (c *Cmd) run(cmd *cobra.Command, args []string) {

	if err := logger.SetLogLevel(c.logLevel, c.logFormat); err != nil {
		log.Err(err).Msg("")
	}

	if c.printDefinition {
		if err := c.performPrintDefinition(); err != nil {
			log.Fatal().Err(err).Msgf("error printing definition")
		}
		return
	}

	if !c.validate && !c.transform {
		log.Fatal().Msgf("behaviour parameter was not provided: expected one of validate transform or print-definition")
	}

	if c.rawMeta == "" {
		log.Fatal().Msgf(`parameter "meta" is required with "validate" or "transform"`)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	eg := &errgroup.Group{}
	eg.Go(func() error {
		defer func() {
			cancel()
		}()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select {
		case <-c:
			log.Debug().Msg("received sigterm")
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
		return nil
	})

	eg.Go(func() (err error) {
		if c.validate {
			err = c.performValidate(ctx)
			log.Debug().Msg("done")
		} else if c.transform {
			err = c.performTransform(ctx)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Warn().Err(err).Msgf("exited with error")
			cancel()
			return err
		}
		close(done)
		log.Debug().Msg("exiting normally")
		return nil
	})

	if err := eg.Wait(); err != nil {
		log.Fatal().Err(err).Msgf("")
	}

}

func (c *Cmd) performPrintDefinition() error {
	if err := json.NewEncoder(os.Stdout).Encode(c.definition); err != nil {
		log.Fatal().Err(err).Msgf("error encoding transformer definition")
	}
	return nil
}

func (c *Cmd) performValidate(ctx context.Context) error {
	transformer, _, warnings, err := c.init(ctx)
	if err != nil {
		return fmt.Errorf("initialization error: %w", err)
	}

	for _, w := range warnings {
		if err = json.NewEncoder(os.Stdout).Encode(w); err != nil {
			return fmt.Errorf("error encoding validation warning: %w", err)
		}
	}

	if warnings.IsFatal() {
		return fmt.Errorf("fatal validation warning")
	}

	warnings, err = transformer.Validate(ctx)
	if err != nil {
		return fmt.Errorf("error validating transformer: %w", err)
	}
	for _, w := range warnings {
		if err = json.NewEncoder(os.Stdout).Encode(w); err != nil {
			return fmt.Errorf("error encoding validation warning: %w", err)
		}
	}

	if warnings.IsFatal() {
		return fmt.Errorf("fatal validation warning")
	}

	return nil
}

func (c *Cmd) performTransform(ctx context.Context) error {
	transformer, driver, warnings, err := c.init(ctx)
	if err != nil {
		return fmt.Errorf("initialization error: %w", err)
	}
	if len(warnings) != 0 && warnings.IsFatal() {
		return fmt.Errorf("fatal validation error")
	}
	rwChan := make(chan struct{}, 1)

	affectedColumnsIdx, _, err := GetAffectedAndTransferringColumns(c.params, driver)
	if err != nil {
		return fmt.Errorf("error getting transferring and affected columns: %w", err)
	}
	transferringColumnsIdx := affectedColumnsIdx

	api, err := NewApi(c.definition.Driver, transferringColumnsIdx, affectedColumnsIdx, driver)
	if err != nil {
		return fmt.Errorf("error inializing api: %w", err)
	}

	api.SetReader(os.Stdin)
	api.SetWriter(os.Stdout)

	record := NewRecord(driver)
	for {
		var row RowDriver
		go func() {
			row, err = api.Decode(ctx)
			rwChan <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rwChan:
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("error decoding data via api: %w", err)
		}

		record.SetRow(row)

		err = transformer.Transform(ctx, record)
		if err != nil {
			return fmt.Errorf("transformation error: %w", err)
		}
		resultRow, err := record.Encode()
		if err != nil {
			return fmt.Errorf("error encoding record: %w", err)
		}

		go func() {
			err = api.Encode(ctx, resultRow)
			rwChan <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rwChan:
		}
		api.Clean()
	}
}

func (c *Cmd) init(ctx context.Context) (Transformer, *Driver, ValidationWarnings, error) {
	meta := &Meta{}
	if err := json.Unmarshal([]byte(c.rawMeta), meta); err != nil {
		return nil, nil, nil, fmt.Errorf("error umarshalling meta: %w", err)
	}
	c.meta = meta
	if meta.Table == nil {
		return nil, nil, nil, fmt.Errorf("error umarshalling meta: empty Table")
	}
	if err := meta.Table.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("metadata validation error: %w", err)
	}

	typeMap := pgtype.NewMap()
	TryRegisterCustomTypes(typeMap, meta.Types, false)

	driver, err := NewDriver(meta.Table, meta.Types, meta.ColumnTypeOverrides)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error initilizing Driver: %w", err)
	}

	params, pw, err := InitParameters(driver, meta.Parameters, c.definition.Parameters, meta.Types)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error parsing parameters: %w", err)
	}
	if pw.IsFatal() {
		return nil, nil, pw, nil
	}

	t, iw, err := c.definition.New(ctx, driver, params)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error initializing transformer: %w", err)
	}
	c.params = params

	return t, driver, iw, nil
}
