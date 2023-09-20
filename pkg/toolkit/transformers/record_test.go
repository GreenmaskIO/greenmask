package transformers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord_ScanAttribute(t *testing.T) {
	row := &TestRowDriver{
		row: []string{"1", "2023-08-27 00:00:00.000000"},
	}
	driver := getDriver()
	r := NewRecord(driver, row)
	var res int
	var expected = 1
	_, err := r.ScanAttribute("id", &res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestRecord_GetAttribute_date(t *testing.T) {
	row := &TestRowDriver{
		row: []string{"1", "2023-08-27 00:00:00.000000", "1234"},
	}
	driver := getDriver()
	r := NewRecord(driver, row)
	res, err := r.GetAttribute("created_at")
	require.NoError(t, err)
	expected := NewValue(time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC), false)
	assert.Equal(t, expected.IsNull, res.IsNull)
	assert.Equal(t, expected.Value, res.Value)
}

func TestRecord_GetAttribute_text(t *testing.T) {
	row := &TestRowDriver{
		row: []string{"1", "2023-08-27 00:00:00.000000", "1234"},
	}
	driver := getDriver()
	r := NewRecord(driver, row)
	res, err := r.GetAttribute("title")
	require.NoError(t, err)
	expected := NewValue("1234", false)
	assert.Equal(t, expected.IsNull, res.IsNull)
	assert.Equal(t, expected.Value, res.Value)
}

func TestRecord_GetTuple(t *testing.T) {
	expected := Tuple{
		"id":         NewValue(int16(1), false),
		"created_at": NewValue(time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC), false),
		"title":      NewValue(nil, true),
	}
	row := &TestRowDriver{
		row: []string{"1", "2023-08-27 00:00:00.000000", testNullSeq},
	}
	driver := getDriver()
	r := NewRecord(driver, row)
	res, err := r.GetTuple()
	require.NoError(t, err)
	for name := range expected {
		assert.Equalf(t, expected[name].IsNull, res[name].IsNull, "wrong IsNull value %s", name)
		assert.Equalf(t, expected[name].Value, res[name].Value, "wrong Value %s", name)
	}

}

func TestRecord_Encode(t *testing.T) {
	row := &TestRowDriver{
		row: []string{"1", "2023-08-27 00:00:00.000001", "test"},
	}
	expected := []byte("2\t2023-08-29 00:00:00.000002\t\\N")
	driver := getDriver()
	r := NewRecord(driver, row)
	err := r.SetAttribute("id", NewValue(int16(2), false))
	require.NoError(t, err)
	err = r.SetAttribute("created_at", NewValue(time.Date(2023, time.August, 29, 0, 0, 0, 2000, time.UTC), false))
	require.NoError(t, err)
	err = r.SetAttribute("title", NewValue(nil, true))
	require.NoError(t, err)
	rowDriver, err := r.Encode()
	require.NoError(t, err)
	res, err := rowDriver.Encode()
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}
