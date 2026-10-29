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

func TestNewWhenCond_column_does_not_exist(t *testing.T) {
	driver := getDriver()

	tests := []struct {
		name            string
		when            string
		meta            map[string]any
		expectedColumns []string
		expectedScope   string
	}{
		{
			name:            "record namespace",
			when:            "record.unknown == 1",
			meta:            map[string]any{},
			expectedColumns: []string{"unknown"},
			expectedScope:   "table",
		},
		{
			name:            "raw_record namespace",
			when:            `raw_record.unknown == "1"`,
			meta:            map[string]any{},
			expectedColumns: []string{"unknown"},
			expectedScope:   "table",
		},
		{
			name:            "bracket access",
			when:            `record["unknown"] == 1`,
			meta:            map[string]any{},
			expectedColumns: []string{"unknown"},
			expectedScope:   "table",
		},
		{
			name:            "transformer scope with existing and missing columns",
			when:            "record.id == 1 && record.missing1 == 2 || raw_record.missing2 == \"3\" || record.missing1 == 4",
			meta:            map[string]any{"Transformer": "RandomInt"},
			expectedColumns: []string{"missing1", "missing2"},
			expectedScope:   "transformer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			whenCond, warns := NewWhenCond(tt.when, driver, tt.meta)
			require.Nil(t, whenCond)
			require.True(t, warns.IsFatal())
			require.Len(t, warns, len(tt.expectedColumns))
			for i, w := range warns {
				require.Equal(t, ErrorValidationSeverity, w.Severity)
				require.Equal(t, "column does not exist", w.Msg)
				require.Equal(t, tt.expectedColumns[i], w.Meta["ColumnName"])
				require.Equal(t, tt.expectedScope, w.Meta["Scope"])
				require.Equal(t, tt.when, w.Meta["WhenCond"])
			}
		})
	}
}

func TestWhenCond_Evaluate_not_boolean(t *testing.T) {
	driver := getDriver()
	record := NewRecord(driver)
	record.SetRow(newTestRowDriver([]string{"1", "2023-08-27 00:00:00.000000", testNullSeq, `{"a": 1}`, "123.0"}))

	whenCond, warns := NewWhenCond("record.id", driver, make(map[string]any))
	require.Empty(t, warns)
	_, err := whenCond.Evaluate(record)
	require.ErrorContains(t, err, "got (int) and value 1")
}
