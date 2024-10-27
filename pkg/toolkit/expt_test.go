package toolkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWhenCond_Evaluate(t *testing.T) {
	driver := getDriver()
	record := NewRecord(driver)
	row := newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000", testNullSeq, `{"a": 1}`, "123.0"})
	record.SetRow(row)

	type test struct {
		name     string
		when     string
		expected bool
	}
	tests := []test{
		{
			name:     "int value equal",
			when:     "record.id == 1",
			expected: true,
		},
		{
			name:     "raw int value equal",
			when:     "raw_record.id == \"1\"",
			expected: true,
		},
		{
			name:     "is null value check",
			when:     "record.title == null",
			expected: true,
		},
		{
			name:     "test date cmp",
			when:     "record.created_at > now()",
			expected: false,
		},
		{
			name:     "test json cmp and sping func",
			when:     `raw_record.json_data | jsonGet("a") == 1`,
			expected: false,
		},
		{
			name:     "check has array func",
			when:     `record.id | has([1, 2, 3, 9223372036854775807])`,
			expected: true,
		},
		{
			name:     "float cmp",
			when:     `record.float_data | has([123.0, 1., 10.])`,
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			whenCond, warns := NewWhenCond(tt.when, driver, make(map[string]any))
			require.Empty(t, warns)
			res, err := whenCond.Evaluate(record)
			require.NoError(t, err)
			require.Equal(t, tt.expected, res)
		})
	}
}
