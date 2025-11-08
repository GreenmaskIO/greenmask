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

package conditions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	"github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestWhenCond_Evaluate(t *testing.T) {
	table := commonmodels.Table{
		Schema: "public",
		Name:   "test",
		Columns: []commonmodels.Column{
			{
				Idx:      0,
				Name:     "id",
				TypeName: "integer",
				TypeOID:  0,
			},
			{
				Idx:      1,
				Name:     "title",
				TypeName: "text",
				TypeOID:  1,
			},
			{
				Idx:      2,
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOID:  2,
			},
			{
				Idx:      3,
				Name:     "json_data",
				TypeName: "jsonb",
				TypeOID:  3,
			},
			{
				Idx:      4,
				Name:     "float_data",
				TypeName: "float8",
				TypeOID:  4,
			},
		},
	}

	type driverExpectation struct {
		columnName   string
		encodedValue any
	}

	type test struct {
		name              string
		when              string
		expected          bool
		driverExpectation driverExpectation
		setupExpectation  func(r *mocks.RecorderMock)
	}
	tests := []test{
		{
			name: "int value equal",
			when: "record.id == 1",
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetColumnValueByName", "id").
					Return(&commonmodels.ColumnValue{
						Value:  1,
						IsNull: false,
					}, nil)
			},
			expected: true,
		},
		{
			name: "raw int value equal",
			when: "raw_record.id == \"1\"",
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetRawColumnValueByName", "id").
					Return(&commonmodels.ColumnRawValue{
						Data:   []byte("1"),
						IsNull: false,
					}, nil)
			},
			expected: true,
		},
		{
			name: "is null value check",
			when: "record.title == null",
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetColumnValueByName", "title").
					Return(&commonmodels.ColumnValue{
						Value:  nil,
						IsNull: true,
					}, nil)
			},
			expected: true,
		},
		{
			name: "test date cmp",
			when: "record.created_at > now()",
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetColumnValueByName", "created_at").
					Return(&commonmodels.ColumnValue{
						Value:  time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
						IsNull: false,
					}, nil)
			},
			expected: false,
		},
		{
			name: "test json cmp and sping func",
			when: `raw_record.json_data | jsonGet("a") == 1`,
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetRawColumnValueByName", "json_data").
					Return(&commonmodels.ColumnRawValue{
						Data:   []byte(`{"a": 1}`),
						IsNull: false,
					}, nil)
			},
			expected: true,
		},
		{
			name: "check has array func",
			when: `record.id | has([1, 2, 3, 9223372036854775807])`,
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetColumnValueByName", "id").
					Return(&commonmodels.ColumnValue{
						Value:  1,
						IsNull: false,
					}, nil)
			},
			expected: true,
		},
		{
			name: "float cmp",
			when: `record.float_data | has([123.0, 1., 10.])`,
			setupExpectation: func(r *mocks.RecorderMock) {
				r.On("GetColumnValueByName", "float_data").
					Return(&commonmodels.ColumnValue{
						Value:  1.,
						IsNull: false,
					}, nil)
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := validationcollector.NewCollector()
			tableDriverMock := mocks.NewTableDriverMock()
			recorderMock := mocks.NewRecorderMock()
			recorderMock.On("TableDriver").Return(tableDriverMock)
			// GetColumnValueByName(columnName string) (*commonmodels.ColumnValue, error) {
			tt.setupExpectation(recorderMock)
			whenCond, warns := NewWhenCond(context.Background(), vc, tt.when, table)
			require.Empty(t, warns)
			res, err := whenCond.Evaluate(recorderMock)
			require.NoError(t, err)
			require.Equal(t, tt.expected, res)
		})
	}
}
