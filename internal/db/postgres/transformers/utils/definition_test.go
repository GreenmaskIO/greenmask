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

package utils

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/toolkit"
)

type TestTransformer struct {
	p map[string]*toolkit.Parameter
}

func (tt *TestTransformer) Init(ctx context.Context) error {
	return nil
}

func (tt *TestTransformer) Done(ctx context.Context) error {
	return nil
}

func (tt *TestTransformer) Transform(ctx context.Context, r *toolkit.Record) (*toolkit.Record, error) {
	return r, nil
}

func (tt *TestTransformer) GetAffectedColumns() map[int]string {
	return nil
}

func NewTestTransformer(ctx context.Context, driver *toolkit.Driver, parameters map[string]*toolkit.Parameter) (
	Transformer, toolkit.ValidationWarnings, error,
) {
	return &TestTransformer{
		p: parameters,
	}, nil, nil
}

func TestDefinition(t *testing.T) {

	TestTransformerDefinition := NewDefinition(
		NewTransformerProperties("test", "simple description"),
		NewTestTransformer,
		toolkit.MustNewParameter("column", "a column Name").
			SetIsColumn(toolkit.NewColumnProperties().
				SetAffected(true).
				SetAllowedColumnTypes("timestamp"),
			),
		toolkit.MustNewParameter("replace", "replacement value").
			SetLinkParameter("column"),
	)

	table := &toolkit.Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*toolkit.Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []toolkit.Constraint{},
	}

	driver, _, err := toolkit.NewDriver(table, nil, nil)
	require.NoError(t, err)

	rawParams := map[string]toolkit.ParamsValue{
		"column":  []byte("created_at"),
		"replace": []byte("2023-08-27 12:08:11.304895"),
	}

	_, warnings, err := TestTransformerDefinition.Instance(context.Background(), driver, rawParams, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
}
