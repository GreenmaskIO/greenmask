package toolkit

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type Cmd struct {
	*cobra.Command
	definition      *Definition
	rawMeta         string
	meta            *Meta
	printDefinition bool
	validate        bool
	transform       bool
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
}

func (c *Cmd) run(cmd *cobra.Command, args []string) {
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(zerolog.DebugLevel).
		With().
		Timestamp().
		Caller().
		Int("pid", os.Getpid()).Logger()

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
		c := make(chan os.Signal, 1) // we need to reserve to buffer size 1, so the notifier are not blocked
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		select {
		case <-c:
		case <-ctx.Done():
			return ctx.Err()
		}
		log.Debug().Msg("received sigterm")
		close(done)
		cancel()
		return nil
	})

	eg.Go(func() (err error) {
		if c.validate {
			err = c.performValidate(ctx)
		} else if c.transform {
			err = c.performTransform(ctx)
		}
		if err != nil {
			log.Warn().Err(err).Msgf("exited with error")
			cancel()
			return err
		}
		<-done
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
		if _, err = os.Stdout.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("error writing to stdout: %w", err)
		}
	}

	if warnings.IsFatal() {
		return nil
	}

	warnings, err = transformer.Validate(ctx)
	if err != nil {
		return fmt.Errorf("error validating transformer: %w", err)
	}
	for _, w := range warnings {
		if err = json.NewEncoder(os.Stdout).Encode(w); err != nil {
			return fmt.Errorf("error encoding validation warning: %w", err)
		}
		if _, err = os.Stdout.Write([]byte{'\n'}); err != nil {
			return fmt.Errorf("error writing to stdout: %w", err)
		}
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
	readCh := make(chan struct{}, 1)
	defer close(readCh)
	r := bufio.NewReader(os.Stdin)
	rr := make(RawRecord, 10)
	for {
		var line []byte
		var err error
		go func() {
			line, _, err = r.ReadLine()
			readCh <- struct{}{}
		}()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-readCh:
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("error reading line from stdout: %w", err)
		}

		if len(line) > 0 {
			if err = json.Unmarshal(line, &rr); err != nil {
				return fmt.Errorf("error umnarshaling raw record: %w", err)
			}
		}
		record := NewRecord(driver, rr)

		record, err = transformer.Transform(ctx, record)
		if err != nil {
			return fmt.Errorf("transformation error: %w", err)
		}
		rawDriver, err := record.Encode()
		if err != nil {
			return fmt.Errorf("error encoding record: %w", err)
		}
		data, err := rawDriver.Encode()
		if err != nil {
			return fmt.Errorf("error encoding raw driver")
		}
		data = append(data, '\n')
		if _, err := os.Stdout.Write(data); err != nil {
			return fmt.Errorf("error writing to stdout: %w", err)
		}
	}
}

func (c *Cmd) init(ctx context.Context) (Transformer, *Driver, ValidationWarnings, error) {
	meta := &Meta{}
	if err := json.Unmarshal([]byte(c.rawMeta), meta); err != nil {
		return nil, nil, nil, fmt.Errorf("error umarshalling meta: %w", err)
	}
	if meta.Table == nil {
		return nil, nil, nil, fmt.Errorf("error umarshalling meta: empty Table")
	}
	if err := meta.Table.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("metadata validation error: %w", err)
	}

	typeMap := pgtype.NewMap()
	TryRegisterCustomTypesV2(typeMap, meta.Types)

	driver, err := NewDriver(typeMap, meta.Table, meta.Types, meta.ColumnTypeOverrides)
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

	return t, driver, iw, nil
}
