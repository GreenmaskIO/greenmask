package cmd

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	transformerstesting "github.com/greenmaskio/greenmask/v1/internal/common/transformers/testing"
	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	mysqldbmsdriver "github.com/greenmaskio/greenmask/v1/internal/mysql/dbmsdriver"
)

func TestDefaultCMDProto_Send(t *testing.T) {
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
			{
				Column:   &columns[2],
				Position: 1,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		rowDriver := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueText](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueText,
		)
		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a"), false),
			commonmodels.NewColumnRawValue([]byte("b"), false),
			commonmodels.NewColumnRawValue([]byte("c"), false),
		}
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		env.SetRecord(t, columnValues...)
		affectedColumns := []*commonmodels.Column{
			&columns[0],
		}
		transferringColumns := []*commonmodels.Column{
			&columns[0],
			&columns[2],
		}
		proto := NewDefaultCMDProto(rowDriver, transferringColumns, affectedColumns)
		record := env.GetRecord()
		reader := bytes.NewBuffer(nil)
		writer := bytes.NewBuffer(nil)
		err := proto.Init(writer, reader)
		require.NoError(t, err)
		ctx := context.Background()
		err = proto.Send(ctx, record)
		require.NoError(t, err)
		assert.JSONEq(t, `[{"d":"a","n":false},{"d":"c","n":false}]`, writer.String())
	})
}

func TestDefaultCMDProto_ReceiveAndApply(t *testing.T) {
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
			{
				Column:   &columns[2],
				Position: 1,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		rowDriver := NewJsonRecordWithAttrIndexes[*JsonAttrRawValueText](
			transferColumn,
			affectedColumn,
			NewJsonAttrRawValueText,
		)
		env := transformerstesting.NewTransformerTestEnvReal(t, nil, columns, nil, nil)
		affectedColumns := []*commonmodels.Column{
			&columns[0],
		}
		transferringColumns := []*commonmodels.Column{
			&columns[0],
			&columns[2],
		}
		proto := NewDefaultCMDProto(rowDriver, transferringColumns, affectedColumns)
		record := env.GetRecord()
		reader := bytes.NewBuffer(nil)
		writer := bytes.NewBuffer(nil)
		err := proto.Init(writer, reader)
		require.NoError(t, err)
		ctx := context.Background()

		columnValues := []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a1"), false),
			commonmodels.NewColumnRawValue([]byte("b1"), false),
			commonmodels.NewColumnRawValue([]byte("c1"), false),
		}
		env.SetRecord(t, columnValues...)
		reader.Write(append([]byte(`[{"d":"a11","n":false},{"d":"c11","n":false}]`), '\n'))
		err = proto.ReceiveAndApply(ctx, record)
		require.NoError(t, err)
		assert.Equal(t, "a11", utils.Must(record.GetRawColumnValueByIdx(0)).String())
		assert.Equal(t, "b1", utils.Must(record.GetRawColumnValueByIdx(1)).String())
		assert.Equal(t, "c1", utils.Must(record.GetRawColumnValueByIdx(2)).String())

		columnValues = []*commonmodels.ColumnRawValue{
			commonmodels.NewColumnRawValue([]byte("a2"), false),
			commonmodels.NewColumnRawValue([]byte("b2"), false),
			commonmodels.NewColumnRawValue([]byte("c2"), false),
		}
		env.SetRecord(t, columnValues...)
		reader.Write(append([]byte(`[{"d":"b22","n":false},{"d":"c22","n":false}]`), '\n'))
		err = proto.ReceiveAndApply(ctx, record)
		require.NoError(t, err)
		assert.Equal(t, "b22", utils.Must(record.GetRawColumnValueByIdx(0)).String())
		assert.Equal(t, "b2", utils.Must(record.GetRawColumnValueByIdx(1)).String())
		assert.Equal(t, "c2", utils.Must(record.GetRawColumnValueByIdx(2)).String())
	})
}
