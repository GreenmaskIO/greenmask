package toolkit

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
)

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
	if c.printDefinition {
		if err := json.NewEncoder(os.Stdout).Encode(c.definition); err != nil {
			log.Fatalf("error encoding transformer definition: %s", err)
		}
		return
	}

	if !c.validate && !c.transform {
		log.Fatalf(`behaviour paramter was not proveded: expected one of validate transform or print-definition`)
	}

	if c.rawMeta == "" {
		log.Fatalf(`parameter "meta" is required with "validate" or "transform"`)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if c.validate {
		if err := c.performValidate(ctx); err != nil {
			log.Fatal(err)
		}
	} else if c.transform {
		if err := c.performTransform(ctx); err != nil {
			log.Fatal(err)
		}
	}
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
			return fmt.Errorf("error reading line from stdout: %w", err)
		}

		rr := make(RawRecord)
		if err = json.Unmarshal(line, &rr); err != nil {
			return fmt.Errorf("error umnarshaling raw record: %w", err)
		}
		record := NewRecord(driver, rr)

		record, err = transformer.Transform(ctx, record)
		if err != nil {
			return fmt.Errorf("transformation error: %w", err)
		}
		rr = record.Row.(RawRecord)

		if err = json.NewEncoder(os.Stdout).Encode(rr); err != nil {
			return fmt.Errorf("error encoding raw record: %w", err)
		}
		if _, err = os.Stdout.Write([]byte{'\n'}); err != nil {
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
		log.Fatalf("error umarshalling meta: empty Table")
	}
	if err := meta.Table.Validate(); err != nil {
		return nil, nil, nil, fmt.Errorf("metadata validation error: %w", err)
	}

	typeMap := pgtype.NewMap()
	driver, err := NewDriver(typeMap, meta.Table, meta.ColumnTypeOverrides)
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
