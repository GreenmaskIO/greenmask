package toolkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawRecordDto(t *testing.T) {
	rawData := []byte(`{"8":{"d":"","n":true},"9":{"d":"","n":true}}`)
	expected := []byte(`{"8":{"d":"test","n":false},"9":{"d":"","n":true}}`)
	rrd := &RawRecordDto{}
	err := json.Unmarshal(rawData, rrd)
	require.NoError(t, err)

	err = rrd.SetColumn(8, NewRawValue([]byte("test"), false))
	require.NoError(t, err)
	err = rrd.SetColumn(10, NewRawValue([]byte("test"), false))
	require.Error(t, err)

	res, err := json.Marshal(rrd)
	require.NoError(t, err)
	require.Equal(t, res, expected)
}
