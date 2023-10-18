package toolkit

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDriver() *Driver {
	table := &Table{
		Schema: "public",
		Name:   "test",
		Oid:    1224,
		Columns: []*Column{
			{
				Name:     "id",
				TypeName: "int2",
				TypeOid:  pgtype.Int2OID,
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "created_at",
				TypeName: "timestamp",
				TypeOid:  pgtype.TimestampOID,
				Num:      2,
				NotNull:  true,
				Length:   -1,
			},
			{
				Name:     "title",
				TypeName: "text",
				TypeOid:  pgtype.TextOID,
				Num:      3,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []Constraint{},
	}
	driver, err := NewDriver(table, nil, nil)
	if err != nil {
		panic(err.Error())
	}
	return driver
}

func TestParameter_Parse_simple(t *testing.T) {

	driver := getDriver()

	p1 := MustNewParameter(
		"simple_param",
		"Simple description",
	)

	warnings, err := p1.Init(driver, nil, []*Parameter{p1}, []byte("1"))
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = 1
	var res int
	err = p1.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestParameter_Parse_with_allowed_pg_types(t *testing.T) {

	driver := getDriver()

	// Check simple column parameter definition positive case
	p1 := MustNewParameter(
		"column",
		"Simple column parameter",
	).SetRequired(true).
		SetIsColumn(&ColumnProperties{
			Nullable:     false,
			Affected:     true,
			AllowedTypes: []string{"date", "timestamp", "timestamptz"},
		})

	//warnings, err := p1.Decode(driver, rawParams, nil, nil)
	warnings, err := p1.Init(driver, nil, []*Parameter{p1}, []byte("created_at"))
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = "created_at"
	res, err := p1.Value()
	assert.Equal(t, expected, res)

	// Check simple column parameter definition negative case
	warnings, err = p1.Init(driver, nil, []*Parameter{p1}, []byte("id"))
	require.NoError(t, err)
	assert.NotEmpty(t, warnings)
	assert.True(t, slices.ContainsFunc(warnings, func(warning *ValidationWarning) bool {
		return warning.Msg == "unsupported column type"
	}))
}

func TestParameter_Parse_with_linked_parameter(t *testing.T) {

	driver := getDriver()

	// Check simple linked parameter definition positive case
	columnParam := MustNewParameter(
		"column",
		"Simple column parameter",
	).SetRequired(true).
		SetIsColumn(NewColumnProperties())

	linkedParam := MustNewParameter(
		"replace",
		"Simple column parameter",
	).SetRequired(true).
		SetLinkParameter("column")

	params := []*Parameter{columnParam, linkedParam}

	warnings, err := columnParam.Init(driver, nil, params, []byte("created_at"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	warnings, err = linkedParam.Init(driver, nil, params, []byte("2023-08-27 00:00:00.000000"))
	require.NoError(t, err)
	assert.Empty(t, warnings)

	res := time.Time{}
	expected := time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)
	err = linkedParam.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}
