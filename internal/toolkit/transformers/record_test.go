package transformers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecord_ScanAttribute(t *testing.T) {
	rawData := []string{"1", "2023-08-27 00:00:00.000000"}
	driver := getDriver()
	r := NewRecord(driver, rawData)
	var res int
	var expected = 1
	err := r.ScanAttribute("id", &res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestRecord_GetAttribute_date(t *testing.T) {
	rawData := []string{"1", "2023-08-27 00:00:00.000000", "1234"}
	driver := getDriver()
	r := NewRecord(driver, rawData)
	res, err := r.GetAttribute("created_at")
	require.NoError(t, err)
	expected := time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, res)
}

func TestRecord_GetAttribute_text(t *testing.T) {
	rawData := []string{"1", "2023-08-27 00:00:00.000000", "1234"}
	driver := getDriver()
	r := NewRecord(driver, rawData)
	res, err := r.GetAttribute("title")
	require.NoError(t, err)
	expected := "1234"
	assert.Equal(t, expected, res)
}

func TestRecord_GetTuple(t *testing.T) {
	expected := Tuple{"id": int16(1), "created_at": time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)}
	rawData := []string{"1", "2023-08-27 00:00:00.000000"}
	driver := getDriver()
	r := NewRecord(driver, rawData)
	res, err := r.GetTuple()
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestRecord_Encode(t *testing.T) {
	rawData := []string{"1", "2023-08-27 00:00:00.000000"}
	driver := getDriver()
	r := NewRecord(driver, rawData)
	r.tuple = Tuple{"id": int16(1), "created_at": time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)}
	res, err := r.Encode()
	require.NoError(t, err)
	assert.Equal(t, rawData, res)
}
