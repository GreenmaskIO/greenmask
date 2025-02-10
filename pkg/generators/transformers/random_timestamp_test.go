package transformers

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

func TestTimestampLimiter_Limit_positive_distance(t *testing.T) {
	minDate := time.Unix(1646812104, 0)
	maxDate := time.Unix(1709970504, 0)
	l, err := NewTimestampLimiter(minDate, maxDate)
	require.NoError(t, err)
	sec, _ := l.Limit(1246474821, 100)
	require.True(t, sec >= minDate.Unix() && sec <= maxDate.Unix())
}

func TestTimestampLimiter_Limit_negative_positive_distance(t *testing.T) {
	minDate := time.Unix(-783101496, 0)
	maxDate := time.Unix(1709970504, 0)
	l, err := NewTimestampLimiter(minDate, maxDate)
	require.NoError(t, err)
	sec, _ := l.Limit(1246474821121, 100)
	require.True(t, sec >= minDate.Unix() && sec <= maxDate.Unix())
}

func TestTimestampLimiter_Limit_negative_negative_distance(t *testing.T) {
	minDate := time.Unix(-2203172704, 0)
	maxDate := time.Unix(-783101496, 0)
	l, err := NewTimestampLimiter(minDate, maxDate)
	require.NoError(t, err)
	sec, _ := l.Limit(1246474821121, 100)
	require.True(t, sec >= minDate.Unix() && sec <= maxDate.Unix())
}

func TestTimestamp_Transform(t *testing.T) {
	minDate := time.Unix(-2203172704, 0)
	maxDate := time.Unix(-783101496, 0)
	l, err := NewTimestampLimiter(minDate, maxDate)
	require.NoError(t, err)
	gen := generators.NewRandomBytes(0, 16)
	tr, err := NewRandomTimestamp("", l)
	require.NoError(t, err)
	err = tr.SetGenerator(gen)
	require.NoError(t, err)
	res, err := tr.Transform(nil, []byte{})
	require.NoError(t, err)
	log.Debug().
		Str("minDate", minDate.String()).
		Str("maxDate", maxDate.String()).
		Str("result", res.String()).
		Msg("")
	require.True(t, res.After(minDate) && res.Before(maxDate))
}
