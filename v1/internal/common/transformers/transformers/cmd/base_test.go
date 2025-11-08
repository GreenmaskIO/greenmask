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

package cmd

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	transformerstesting "github.com/greenmaskio/greenmask/v1/internal/common/transformers/testing"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestTransformerBase_Transform(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:      0,
				Name:     "first_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      1,
				Name:     "last_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      2,
				Name:     "middle_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)

		settings := &RowDriverSetting{
			Name: RowDriverNameText,
		}
		proto, err := NewProto(settings, transferColumn, affectedColumn)
		require.NoError(t, err)
		base := NewTransformerBase("test", 0, 100*time.Second, &env.Table, proto)
		ctx := context.Background()
		err = base.Init(ctx, "./test/transformer-success.sh", nil)
		require.NoError(t, err)

		// 1st iteration
		err = base.Transform(ctx, env.Recorder)
		require.NoError(t, err)
		val, err := env.Recorder.GetRawColumnValueByName(columns[0].Name)
		require.NoError(t, err)
		require.False(t, val.IsNull)
		require.Equal(t, "0cc175b9c0f1b6a831c399e269772661", string(val.Data))

		// 2nd iteration
		err = base.Transform(ctx, env.Recorder)
		require.NoError(t, err)
		val, err = env.Recorder.GetRawColumnValueByName(columns[0].Name)
		require.NoError(t, err)
		require.False(t, val.IsNull)
		require.Equal(t, "d7afde3e7059cd0a0fe09eec4b0008cd", string(val.Data))

		err = base.Done(ctx)
		require.NoError(t, err)
	})

	t.Run("init error", func(t *testing.T) {
		columns := []commonmodels.Column{
			{
				Idx:      0,
				Name:     "first_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      1,
				Name:     "last_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
			{
				Idx:      2,
				Name:     "middle_name",
				TypeName: mysqldbmsdriver.TypeText,
				TypeOID:  mysqldbmsdriver.VirtualOidText,
				Length:   0,
			},
		}
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)

		settings := &RowDriverSetting{
			Name: RowDriverNameText,
		}
		proto, err := NewProto(settings, transferColumn, affectedColumn)
		require.NoError(t, err)
		base := NewTransformerBase("test", 0, 100*time.Second, &env.Table, proto)
		ctx := context.Background()
		err = base.Init(ctx, "./test/transformer-error.sh", nil)
		require.NoError(t, err)

		// 1st iteration
		err = base.Transform(ctx, env.Recorder)
		assert.Error(t, err)
		assert.ErrorIs(t, err, io.EOF)

		err = base.Done(ctx)
		require.NoError(t, err)
	})
}
