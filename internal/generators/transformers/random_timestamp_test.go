package transformers

import (
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
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

func TestTimestampLimiter_Limit_nanoseconds(t *testing.T) {
	tests := []struct {
		name    string
		minDate time.Time
		maxDate time.Time
	}{
		{name: "different seconds", minDate: time.Unix(100, 900_000_000), maxDate: time.Unix(101, 100_000_000)},
		{name: "same second", minDate: time.Unix(100, 500_000_000), maxDate: time.Unix(100, 600_000_000)},
		{name: "negative seconds", minDate: time.Unix(-101, 900_000_000), maxDate: time.Unix(-100, 100_000_000)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := NewTimestampLimiter(tt.minDate, tt.maxDate)
			require.NoError(t, err)
			for sec := int64(0); sec < 10; sec++ {
				for _, nano := range []int64{0, 1, 123_456_789, 499_999_999, 999_999_999} {
					s, n := l.Limit(sec, nano)
					res := time.Unix(s, n)
					require.False(t, res.Before(tt.minDate), "result %s is before min %s", res, tt.minDate)
					require.False(t, res.After(tt.maxDate), "result %s is after max %s", res, tt.maxDate)
				}
			}
		})
	}
}

func TestTimestamp_Transform_utc(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	origLocal := time.Local
	time.Local = loc
	defer func() { time.Local = origLocal }()

	minDate := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2020, 1, 1, 3, 0, 0, 0, time.UTC)
	l, err := NewTimestampLimiter(minDate, maxDate)
	require.NoError(t, err)
	tr, err := NewRandomTimestamp(DayTruncateName, l)
	require.NoError(t, err)
	require.NoError(t, tr.SetGenerator(generators.NewRandomBytes(0, 16)))
	for i := 0; i < 20; i++ {
		res, err := tr.Transform(nil, []byte{})
		require.NoError(t, err)
		require.Equal(t, time.UTC, res.Location())
		require.True(t, res.Equal(minDate), "expected %s got %s", minDate, res)
	}
}

func TestDateTruncater_Truncate_sub_second(t *testing.T) {
	v := time.Date(2020, 1, 2, 3, 4, 5, 123_456_789, time.UTC)
	tests := []struct {
		part     string
		expected time.Time
	}{
		{part: MillisecondTruncateName, expected: time.Date(2020, 1, 2, 3, 4, 5, 123_000_000, time.UTC)},
		{part: MicrosecondTruncateName, expected: time.Date(2020, 1, 2, 3, 4, 5, 123_456_000, time.UTC)},
		{part: NanosecondTruncateName, expected: v},
		{part: SecondTruncateName, expected: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.part, func(t *testing.T) {
			dt, err := NewDateTruncater(tt.part)
			require.NoError(t, err)
			require.Equal(t, tt.expected, dt.Truncate(v))
		})
	}
}
