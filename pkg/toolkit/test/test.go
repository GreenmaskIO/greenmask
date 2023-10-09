package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var testTransformerDefinition = toolkit.NewDefinition(
	"TestTransformer",
	NewTestTransformer,
).SetValidate(true).
	AddParameter(
		toolkit.MustNewParameter("column", "test desc").
			SetIsColumn(&toolkit.ColumnProperties{
				AllowedTypes: []string{"date", "timestamp", "timestamptz"},
			}).
			SetRequired(true),
	)

type TestTransformer struct {
	columnName string
	driver     *toolkit.Driver
	parameters map[string]*toolkit.Parameter
}

func NewTestTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (
	toolkit.Transformer, toolkit.ValidationWarnings, error) {
	c := parameters["column"]
	var columnName string
	if err := c.Scan(&columnName); err != nil {
		return nil, nil, fmt.Errorf("error scanning column name")
	}

	return &TestTransformer{
		columnName: columnName,
	}, nil, nil
}

func (tt *TestTransformer) Validate(ctx context.Context) (toolkit.ValidationWarnings, error) {
	return nil, nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {

	_, err := r.GetAttribute(tt.columnName)
	if err != nil {
		return nil, fmt.Errorf("error scanning attrbite: %w", err)
	}
	now := time.Now()
	if err = r.SetAttribute(tt.columnName, &now); err != nil {
		return nil, fmt.Errorf("error setting attrbite: %w", err)
	}

	return r, nil
}

func main() {
	cmd := toolkit.NewCmd(testTransformerDefinition)
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
