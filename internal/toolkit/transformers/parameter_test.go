package transformers

import (
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getDriver() *Driver {
	typeMap := pgtype.NewMap()
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
				Num:      1,
				NotNull:  true,
				Length:   -1,
			},
		},
		Constraints: []Constraint{},
	}
	driver, err := NewDriver(typeMap, table)
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
		new(int),
		nil,
	)

	rawParams := map[string][]byte{
		"simple_param": []byte("1"),
	}

	warnings, err := p1.Parse(driver, rawParams, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = 1
	res := p1.Value()
	assert.Equal(t, &expected, res)
}

func TestParameter_Parse_with_allowed_pg_types(t *testing.T) {

	driver := getDriver()

	// Check simple column parameter definition positive case

	rawParams := map[string][]byte{
		"column": []byte("created_at"),
	}

	p1 := MustNewParameter(
		"column",
		"Simple column parameter",
		new(string),
		nil,
	).SelAllowedDbTypes([]string{"timestamp"}).
		SetRequired(true).
		SetIsColumn(&ColumnProperties{
			Nullable: false,
			Affected: true,
		})

	warnings, err := p1.Parse(driver, rawParams, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)
	var expected = "created_at"
	res := p1.Value()
	assert.Equal(t, &expected, res)

	// Check simple column parameter definition negative case
	rawParams = map[string][]byte{
		"column": []byte("id"),
	}

	warnings, err = p1.Parse(driver, rawParams, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, warnings)
	assert.True(t, slices.ContainsFunc(warnings, func(warning *ValidationWarning) bool {
		return warning.Msg == "unsupported column type"
	}))
}

func TestParameter_Parse_with_linked_parameter(t *testing.T) {

	driver := getDriver()

	rawParams := map[string][]byte{
		"column":  []byte("created_at"),
		"replace": []byte("2023-08-27 00:00:00.000000"),
	}

	// Check simple linked parameter definition positive case
	columnParam := MustNewParameter(
		"column",
		"Simple column parameter",
		new(string),
		nil,
	).SelAllowedDbTypes([]string{"timestamp"}).
		SetRequired(true).
		SetIsColumn(NewColumnProperties())

	warnings, err := columnParam.Parse(driver, rawParams, nil)
	require.NoError(t, err)
	assert.Empty(t, warnings)

	linkedParam := MustNewParameter(
		"replace",
		"Simple column parameter",
		&time.Time{},
		nil,
	).SelAllowedDbTypes([]string{"timestamp"}).
		SetRequired(true).
		SetLinkParameter("column")

	warnings, err = linkedParam.Parse(driver, rawParams, map[string]*Parameter{"column": columnParam})
	require.NoError(t, err)
	assert.Empty(t, warnings)
	res := time.Time{}
	expected := time.Date(2023, time.August, 27, 0, 0, 0, 0, time.UTC)
	err = linkedParam.Scan(&res)
	require.NoError(t, err)
	assert.Equal(t, expected, res)
}
