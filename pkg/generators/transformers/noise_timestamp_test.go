package transformers

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

func TestNoiseTimestamp_Transform(t *testing.T) {
	//fmt.Printf("%d", time.Now().Unix())
	original := time.Unix(1712668244, 0)
	minRatio := 10 * (time.Hour * 24) // 10 days
	maxRatio := 90 * (time.Hour * 24) // 90 days

	expectedMinValue := original.Add(-80 * (time.Hour * 24)) // now - 10 days
	expectedMaxValue := original.Add(+80 * (time.Hour * 24)) // now + 10 days

	l, err := NewNoiseTimestampLimiter(&expectedMinValue, &expectedMaxValue)
	require.NoError(t, err)

	tr, err := NewNoiseTimestamp(minRatio, maxRatio, "", l)
	require.NoError(t, err)
	g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
	require.NoError(t, err)
	err = tr.SetGenerator(g)
	require.NoError(t, err)
	res, err := tr.Transform(nil, original)
	require.NoError(t, err)
	log.Debug().
		Time("original", original).
		Time("transformed", res).
		Dur("minRatio", minRatio).
		Dur("maxRatio", maxRatio).
		Time("minExpected", expectedMinValue).
		Time("maxExpected", expectedMaxValue).
		Msg("")
	require.True(t, res.After(expectedMinValue.Add(-1)) && res.Before(expectedMaxValue.Add(1)))
}
