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
	SetMode(toolkit.TextModeName).
	AddParameter(
		toolkit.MustNewParameter("column", "test desc").
			SetIsColumn(
				toolkit.NewColumnProperties().
					SetAllowedColumnTypes("date", "timestamp", "timestamptz").
					SetSkipOriginalData(true).
					SetAffected(true),
			).
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
	//return toolkit.ValidationWarnings{toolkit.NewValidationWarning().SetMsg("test validation")}, nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	now := time.Now()
	if err := r.SetAttributeByName(tt.columnName, &now); err != nil {
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
