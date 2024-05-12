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

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

var testTransformerDefinition = toolkit.NewTransformerDefinition(
	"TestTransformer",
	NewTestTransformer,
).SetValidate(true).
	SetDescription("Simple test transformer").
	SetMode(&(*toolkit.DefaultRowDriverParams)).
	AddParameter(
		toolkit.MustNewParameterDefinition("column", "test desc").
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
}

func NewTestTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]toolkit.Parameterizer) (
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

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) error {
	now := time.Now()
	if err := r.SetColumnValueByName(tt.columnName, &now); err != nil {
		return fmt.Errorf("error setting attrbite: %w", err)
	}

	return nil
}

func main() {
	cmd := toolkit.NewCmd(testTransformerDefinition)
	if err := cmd.Execute(); err != nil {
		log.Fatal().Err(err).Msg("")
	}
}
