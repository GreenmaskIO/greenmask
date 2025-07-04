package tabledriver

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/v1/internal/common/mocks"
	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
	validationcollector "github.com/greenmaskio/greenmask/v1/internal/common/validationcollector"
)

func TestNewTableDriver(t *testing.T) {
	t.Run("common", func(t *testing.T) {
		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
				{Name: "col2", TypeOID: 2, TypeName: "text"},
			},
		}

		typeOverride := map[string]string{}

		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true)
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(2)).Return(true)
		vc := validationcollector.NewCollector()
		actual, err := New(vc, mockDriver, table, typeOverride)

		assert.NoError(t, err)
		assert.NotNil(t, actual)
		require.Equal(t, vc.Len(), 0)

		// Check columnMap
		assert.Equal(t, actual.columnMap, map[string]*commonmodels.Column{
			"col1": {Name: "col1", TypeOID: 1, TypeName: "int"},
			"col2": {Name: "col2", TypeOID: 2, TypeName: "text"},
		})

		// Check columnIdxMap
		assert.Equal(t, actual.columnIdxMap, map[string]int{
			"col1": 0,
			"col2": 1,
		})

		// Check unsupported columns
		assert.Len(t, actual.unsupportedColumnNames, 0)
		assert.Len(t, actual.unsupportedColumnIdxs, 0)

		// Check type override
		assert.Len(t, actual.typeOverride, 0)
		assert.Len(t, actual.columnTypeOidOverrideMap, 0)
		assert.Len(t, actual.columnIdxTypeOidOverrideMap, 0)

		// Check maxIdx
		assert.Equal(t, actual.maxIdx, 1)
	})

	t.Run("with unsupported columns", func(t *testing.T) {
		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
				{Name: "col2", TypeOID: 2, TypeName: "unknown_type"},
			},
		}

		typeOverride := map[string]string{}

		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true)
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(2)).Return(false)

		vc := validationcollector.NewCollector()
		actual, err := New(vc, mockDriver, table, typeOverride)

		assert.NoError(t, err)
		assert.NotNil(t, actual)
		// One warning for unsupported column type is expected.
		require.Equal(t, vc.Len(), 1)
		warning := vc.GetWarnings()[0]
		assert.Equal(t, warning.Severity, commonmodels.ValidationSeverityWarning)
		assert.Equal(
			t,
			warning.Msg,
			"cannot match encoder/decoder for type: encode and decode operations is not supported",
		)

		// Check columnMap
		assert.Equal(t, actual.columnMap, map[string]*commonmodels.Column{
			"col1": {Name: "col1", TypeOID: 1, TypeName: "int"},
			"col2": {Name: "col2", TypeOID: 2, TypeName: "unknown_type"},
		})

		// Check columnIdxMap
		assert.Equal(t, actual.columnIdxMap, map[string]int{
			"col1": 0,
			"col2": 1,
		})

		// Check unsupported columns
		assert.Equal(t, actual.unsupportedColumnNames, map[string]string{
			"col2": "unknown_type",
		})
		assert.Equal(t, actual.unsupportedColumnIdxs, map[int]string{
			1: "unknown_type",
		})

		// Check type override
		assert.Len(t, actual.typeOverride, 0)
		assert.Len(t, actual.columnTypeOidOverrideMap, 0)
		assert.Len(t, actual.columnIdxTypeOidOverrideMap, 0)

		// Check maxIdx
		assert.Equal(t, actual.maxIdx, 1)
	})

	t.Run("with type override and type exists", func(t *testing.T) {
		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
				{Name: "col2", TypeOID: 100, TypeName: "unknown_type"},
			},
		}

		typeOverride := map[string]string{
			"col2": "text", // Override to a known type
		}

		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(100)).Return(false).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()

		vc := validationcollector.NewCollector()
		actual, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		// One warning for unsupported column type is expected.
		require.Equal(t, vc.Len(), 0)

		// Check columnMap
		assert.Equal(t, actual.columnMap, map[string]*commonmodels.Column{
			"col1": {Name: "col1", TypeOID: 1, TypeName: "int"},
			"col2": {Name: "col2", TypeOID: 100, TypeName: "unknown_type"},
		})

		// Check columnIdxMap
		assert.Equal(t, actual.columnIdxMap, map[string]int{
			"col1": 0,
			"col2": 1,
		})

		// Check unsupported columns
		assert.Empty(t, actual.unsupportedColumnNames)
		assert.Empty(t, actual.unsupportedColumnIdxs)

		// Check type override
		assert.Equal(t, map[string]string{"col2": "text"}, actual.typeOverride)
		assert.Equal(t, map[string]commonmodels.VirtualOID{"col2": commonmodels.VirtualOID(2)}, actual.columnTypeOidOverrideMap)
		assert.Equal(t, actual.columnIdxTypeOidOverrideMap, map[int]commonmodels.VirtualOID{1: commonmodels.VirtualOID(2)})

		// Check maxIdx
		assert.Equal(t, actual.maxIdx, 1)
	})

	t.Run("with type override and does type exist", func(t *testing.T) {
		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
				{Name: "col2", TypeOID: 100, TypeName: "unknown_type"},
			},
		}

		typeOverride := map[string]string{
			"col2": "text", // Override to a known type
		}

		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(100)).Return(false).Once()
		mockDriver.On("TypeExistsByName", "text").Return(false).Once()

		vc := validationcollector.NewCollector()
		actual, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)
		assert.NotNil(t, actual)
		// One warning for unsupported column type is expected.
		require.Equal(t, vc.Len(), 1)
		assert.Equal(t, commonmodels.ValidationSeverityError, vc.GetWarnings()[0].Severity)
		assert.Equal(
			t,
			vc.GetWarnings()[0].Msg,
			"unknown or unsupported overridden type name by DBMS driver:"+
				" encode and decode operations are not supported",
		)

		// Check columnMap
		assert.Equal(t, actual.columnMap, map[string]*commonmodels.Column{
			"col1": {Name: "col1", TypeOID: 1, TypeName: "int"},
			"col2": {Name: "col2", TypeOID: 100, TypeName: "unknown_type"},
		})

		// Check columnIdxMap
		assert.Equal(t, actual.columnIdxMap, map[string]int{
			"col1": 0,
			"col2": 1,
		})

		// Check unsupported columns
		assert.Equal(t, actual.unsupportedColumnNames, map[string]string{
			"col2": "unknown_type",
		})
		assert.Equal(t, actual.unsupportedColumnIdxs, map[int]string{
			1: "unknown_type",
		})

		// Check type override
		assert.Len(t, actual.typeOverride, 1)
		assert.Len(t, actual.columnTypeOidOverrideMap, 0)
		assert.Len(t, actual.columnIdxTypeOidOverrideMap, 0)

		// Check maxIdx
		assert.Equal(t, actual.maxIdx, 1)
	})
}

func TestDriver_EncodeValueByColumnIdx(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("EncodeValueByTypeOid", commonmodels.VirtualOID(1), "value", []byte(nil)).Return([]byte("encoded"), nil).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		result, err := driver.EncodeValueByColumnIdx(0, "value", nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("encoded"), result)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("EncodeValueByTypeOid", commonmodels.VirtualOID(2), "value", []byte(nil)).Return([]byte("encoded"), nil).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		result, err := driver.EncodeValueByColumnIdx(0, "value", nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("encoded"), result)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		_, err = driver.EncodeValueByColumnIdx(0, "value", nil)
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}

func TestDriver_EncodeValueByColumnName(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("EncodeValueByTypeOid", commonmodels.VirtualOID(1), "value", []byte(nil)).Return([]byte("encoded"), nil).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		result, err := driver.EncodeValueByColumnName("col1", "value", nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("encoded"), result)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("EncodeValueByTypeOid", commonmodels.VirtualOID(2), "value", []byte(nil)).Return([]byte("encoded"), nil).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		result, err := driver.EncodeValueByColumnName("col1", "value", nil)
		assert.NoError(t, err)
		assert.Equal(t, []byte("encoded"), result)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		_, err = driver.EncodeValueByColumnName("col1", "value", nil)
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}

func TestDriver_ScanValueByColumnIdx(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		actual := ""
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("ScanValueByTypeOid", commonmodels.VirtualOID(1), []byte("value"), &actual).
			Return(nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		err = driver.ScanValueByColumnIdx(0, []byte("value"), &actual)
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		actual := ""
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("ScanValueByTypeOid", commonmodels.VirtualOID(2), []byte("value"), &actual).
			Return(nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		err = driver.ScanValueByColumnIdx(0, []byte("value"), &actual)
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		var actual string
		err = driver.ScanValueByColumnIdx(0, []byte("value"), &actual)
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}

func TestDriver_ScanValueByColumnName(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		actual := ""
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("ScanValueByTypeOid", commonmodels.VirtualOID(1), []byte("value"), &actual).
			Return(nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		err = driver.ScanValueByColumnName("col1", []byte("value"), &actual)
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		actual := ""
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("ScanValueByTypeOid", commonmodels.VirtualOID(2), []byte("value"), &actual).
			Return(nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		err = driver.ScanValueByColumnName("col1", []byte("value"), &actual)
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		var actual string
		err = driver.ScanValueByColumnName("col1", []byte("value"), &actual)
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}

func TestDriver_DecodeValueByColumnName(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("DecodeValueByTypeOid", commonmodels.VirtualOID(1), []byte("value")).
			Return("value", nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		actual, err := driver.DecodeValueByColumnName("col1", []byte("value"))
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("DecodeValueByTypeOid", commonmodels.VirtualOID(2), []byte("value")).
			Return("value", nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		actual, err := driver.DecodeValueByColumnName("col1", []byte("value"))
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		_, err = driver.DecodeValueByColumnName("col1", []byte("value"))
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}

func TestDriver_DecodeValueByColumnIdx(t *testing.T) {
	t.Run("common case", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("DecodeValueByTypeOid", commonmodels.VirtualOID(1), []byte("value")).
			Return("value", nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "int"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		actual, err := driver.DecodeValueByColumnIdx(0, []byte("value"))
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("type overridden", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(true).Once()
		mockDriver.On("TypeExistsByName", "text").Return(true).Once()
		mockDriver.On("GetTypeOid", "text").Return(commonmodels.VirtualOID(2), nil).Once()
		mockDriver.On("DecodeValueByTypeOid", commonmodels.VirtualOID(2), []byte("value")).
			Return("value", nil).
			Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		typeOverride := map[string]string{"col1": "text"}
		driver, err := New(vc, mockDriver, table, typeOverride)
		assert.NoError(t, err)

		actual, err := driver.DecodeValueByColumnIdx(0, []byte("value"))
		assert.NoError(t, err)
		assert.Equal(t, "value", actual)

		mockDriver.AssertExpectations(t)
	})

	t.Run("unsupported type", func(t *testing.T) {
		mockDriver := mocks.NewDBMSDriverMock()
		mockDriver.On("TypeExistsByOid", commonmodels.VirtualOID(1)).Return(false).Once()

		table := &commonmodels.Table{
			Schema: "public",
			Name:   "test_table",
			Columns: []commonmodels.Column{
				{Name: "col1", TypeOID: 1, TypeName: "unknown_type"},
			},
		}

		vc := validationcollector.NewCollector()
		driver, err := New(vc, mockDriver, table, nil)
		assert.NoError(t, err)

		_, err = driver.DecodeValueByColumnIdx(0, []byte("value"))
		assert.Error(t, err)

		mockDriver.AssertExpectations(t)
	})
}
