package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func TestNewProto(t *testing.T) {
	columns := []commonmodels.Column{
		{
			Idx:  0,
			Name: "first_name",
		},
		{
			Idx:  1,
			Name: "last_name",
		},
		{
			Idx:  2,
			Name: "middle_name",
		},
	}

	t.Run("json mode index byte", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameJson,
			JsonConfig: JsonRowDriverConfig{
				DataFormat:   JsonRowDriverDataFormatBytes,
				ColumnFormat: JsonRowDriverColumnFormatByIndexes,
			},
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*JsonRecordWithAttrIndexes[*JsonAttrRawValueBytes])
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("json mode index text", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameJson,
			JsonConfig: JsonRowDriverConfig{
				DataFormat:   JsonRowDriverDataFormatText,
				ColumnFormat: JsonRowDriverColumnFormatByIndexes,
			},
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*JsonRecordWithAttrIndexes[*JsonAttrRawValueText])
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("json mode names bytes", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameJson,
			JsonConfig: JsonRowDriverConfig{
				DataFormat:   JsonRowDriverDataFormatBytes,
				ColumnFormat: JsonRowDriverColumnFormatByNames,
			},
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*JsonRecordWithAttrNames[*JsonAttrRawValueBytes])
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("json mode names text", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameJson,
			JsonConfig: JsonRowDriverConfig{
				DataFormat:   JsonRowDriverDataFormatText,
				ColumnFormat: JsonRowDriverColumnFormatByNames,
			},
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*JsonRecordWithAttrNames[*JsonAttrRawValueText])
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("text", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameText,
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*TextRecord)
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("text too many columns", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameText,
		}
		_, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.Error(t, err)
		require.ErrorIs(t, err, errMoreThanOneColumn)
	})

	t.Run("csv", func(t *testing.T) {
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
		settings := &RowDriverSetting{
			Name: RowDriverNameCSV,
		}
		proto, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.NoError(t, err)
		protoRowDriver, ok := proto.(*DefaultCMDProto).rowDriver.(*CSVRecord)
		require.True(t, ok)
		require.NotNil(t, protoRowDriver)
	})

	t.Run("err conflict", func(t *testing.T) {
		transferColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
			{
				Column:   &columns[1],
				Position: 0,
			},
		}
		affectedColumn := []*ColumnMapping{
			{
				Column:   &columns[0],
				Position: 0,
			},
		}
		settings := &RowDriverSetting{
			Name: RowDriverNameCSV,
		}
		_, err := NewProto(
			settings,
			transferColumn,
			affectedColumn,
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, errConflictingColumnMappings)
	})
}
