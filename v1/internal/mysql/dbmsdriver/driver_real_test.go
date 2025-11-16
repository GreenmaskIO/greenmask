package dbmsdriver

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/greenmaskio/greenmask/v1/internal/common/utils"
	"github.com/greenmaskio/greenmask/v1/internal/testutils"
)

//func TestMySQL84(t *testing.T) {
//	suite.Run(t, new(driverSuite).SetImage("mysql:8.4"))
//}

type mysqlSuite struct {
	testutils.MySQLContainerSuite
}

//func (s *mysqlSuite) SetupSuite() {
//	s.SetMigrationUser(mysqlRootUser, mysqlRootPass).
//		SetRootUser(mysqlRootUser, mysqlRootPass).
//		SetupSuite()
//}

func (s *mysqlSuite) TearDownSuite() {
	s.MySQLContainerSuite.TearDownSuite()
}

func (s *mysqlSuite) TestMySQLContainerSuite() {
	s.Require().NotNil(s.Container)
}

func TestMySQL80(t *testing.T) {
	s := new(mysqlSuite)
	s.SetImage("mysql:8.0")
	suite.Run(t, s)
}

func TestMySQL84(t *testing.T) {
	s := new(mysqlSuite)
	s.SetImage("mysql:8.4")
	suite.Run(t, s)
}

type ValueCase struct {
	Name  string // "min", "max", "zero", "empty", ...
	Input any    // the value we want to insert into the DB and expect from Decode/Scan
}

type ScanDestCase struct {
	Name    string     // "int64", "sql.NullString", "time.Time", ...
	NewDest func() any // creates a destination variable (a pointer)
	Compare func(t *testing.T, expected any, dest any)
}

type TypeCase struct {
	Name       string      // logical name: "int", "bigint", "varchar", "datetime", ...
	TypeName   string      // type name used by DBMSDriver: "INT", "BIGINT", "VARCHAR", "DATETIME"
	ColumnType string      // how the column is created in DDL: "INT", "BIGINT", "VARCHAR(255)", "DATETIME(6)", "DECIMAL(10,2)"...
	Values     []ValueCase // list of test input values
	DecodeCmp  func(t *testing.T, expected any, decoded any)
	ScanDests  []ScanDestCase // supported Scan destinations for this type
}

func (s *mysqlSuite) createTempTable(
	ctx context.Context,
	tc TypeCase,
	db *sql.DB,
) (tableName string, cleanup func()) {

	tableName = fmt.Sprintf("tmp_%s_%d", strings.ToLower(tc.Name), time.Now().UnixNano())
	ddl := fmt.Sprintf("CREATE TABLE `%s` (data %s)", tableName, tc.ColumnType)

	_, err := db.ExecContext(ctx, ddl)
	s.Require().NoError(err)

	cleanup = func() {
		_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
		s.Require().NoError(err)
	}

	return tableName, cleanup
}

func (s *mysqlSuite) insertAndFetchRaw(
	ctx context.Context,
	tableName string,
	value any,
) []byte {
	db, err := s.GetConnection(ctx)
	s.Require().NoError(err)

	_, err = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", tableName))
	s.Require().NoError(err)

	_, err = db.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO `%s` (data) VALUES (?)", tableName),
		value,
	)
	s.Require().NoError(err)

	row := db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT data FROM `%s` LIMIT 1", tableName),
	)

	var raw []byte
	err = row.Scan(&raw)
	s.Require().NoError(err)

	return append([]byte(nil), raw...)
}

var allTypeCases = []TypeCase{
	// ---------- INTEGER TYPES ----------
	{
		Name:       TypeTinyInt,
		TypeName:   TypeTinyInt,
		ColumnType: "TINYINT",
		Values: []ValueCase{
			{"min", int64(-128)},
			{"zero", int64(0)},
			{"max", int64(127)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeSmallInt,
		TypeName:   TypeSmallInt,
		ColumnType: "SMALLINT",
		Values: []ValueCase{
			{"min", int64(-32768)},
			{"zero", int64(0)},
			{"max", int64(32767)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeMediumInt,
		TypeName:   TypeMediumInt,
		ColumnType: "MEDIUMINT",
		Values: []ValueCase{
			{"min", int64(-8388608)},
			{"zero", int64(0)},
			{"max", int64(8388607)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeInt,
		TypeName:   TypeInt,
		ColumnType: "INT",
		Values: []ValueCase{
			{"min", int64(-2147483648)},
			{"zero", int64(0)},
			{"max", int64(2147483647)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeBigInt,
		TypeName:   TypeBigInt,
		ColumnType: "BIGINT",
		Values: []ValueCase{
			{"min", int64(math.MinInt64)},
			{"zero", int64(0)},
			{"max", int64(math.MaxInt64)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},

	// ---------- NUMERIC / DECIMAL ----------
	{
		Name:       TypeNumeric,
		TypeName:   TypeNumeric,
		ColumnType: "NUMERIC(10,4)",
		Values: []ValueCase{
			{"neg", utils.Must(decimal.NewFromString("-10.0000"))},
			{"zero", utils.Must(decimal.NewFromString("0.0000"))},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(decimal.Decimal)
			require.IsType(t, decimal.Decimal{}, decoded)
			diff := cmp.Diff(exp, decoded)
			if diff != "" {
				t.Errorf("mismatch (-expected +actual):\n%s", diff)
			}
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v decimal.Decimal; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(decimal.Decimal)
					got := dest.(*decimal.Decimal)
					diff := cmp.Diff(exp, *got)
					if diff != "" {
						t.Errorf("mismatch (-expected +actual):\n%s", diff)
					}
				},
			},
		},
	},
	{
		Name:       TypeDecimal,
		TypeName:   TypeDecimal,
		ColumnType: "DECIMAL(10,2)",
		Values: []ValueCase{
			{"neg", utils.Must(decimal.NewFromString("-10.0000"))},
			{"zero", utils.Must(decimal.NewFromString("0.0000"))},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(decimal.Decimal)
			require.IsType(t, decimal.Decimal{}, decoded)
			diff := cmp.Diff(exp, decoded)
			if diff != "" {
				t.Errorf("mismatch (-expected +actual):\n%s", diff)
			}
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "decimal",
				NewDest: func() any { var v decimal.Decimal; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(decimal.Decimal)
					got := dest.(*decimal.Decimal)
					diff := cmp.Diff(exp, *got)
					if diff != "" {
						t.Errorf("mismatch (-expected +actual):\n%s", diff)
					}
				},
			},
		},
	},

	// ---------- FLOATING POINT ----------
	{
		Name:       TypeFloat,
		TypeName:   TypeFloat,
		ColumnType: "FLOAT",
		Values: []ValueCase{
			{"pos", float64(123.5)},
			{"neg", float64(-10.25)},
			{"zero", float64(0)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(float64)
			require.IsType(t, float64(0), decoded)
			got := decoded.(float64)
			require.InDelta(t, exp, got, 1e-5)
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "float64",
				NewDest: func() any { var v float64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(float64)
					got := dest.(*float64)
					require.InDelta(t, exp, *got, 1e-5)
				},
			},
		},
	},
	{
		Name:       TypeDouble,
		TypeName:   TypeDouble,
		ColumnType: "DOUBLE",
		Values: []ValueCase{
			{"pos", float64(123.5)},
			{"neg", float64(-10.25)},
			{"zero", float64(0)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(float64)
			require.IsType(t, float64(0), decoded)
			got := decoded.(float64)
			require.InDelta(t, exp, got, 1e-9)
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "float64",
				NewDest: func() any { var v float64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(float64)
					got := dest.(*float64)
					require.InDelta(t, exp, *got, 1e-9)
				},
			},
		},
	},
	{
		Name:       TypeReal,
		TypeName:   TypeReal,
		ColumnType: "REAL",
		Values: []ValueCase{
			{"pos", float64(123.5)},
			{"neg", float64(-10.25)},
			{"zero", float64(0)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(float64)
			require.IsType(t, float64(0), decoded)
			got := decoded.(float64)
			require.InDelta(t, exp, got, 1e-5)
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "float64",
				NewDest: func() any { var v float64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(float64)
					got := dest.(*float64)
					require.InDelta(t, exp, *got, 1e-5)
				},
			},
		},
	},

	// ---------- DATE / TIME ----------
	{
		Name:       TypeDate,
		TypeName:   TypeDate,
		ColumnType: "DATE",
		Values: []ValueCase{
			{"simple", time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(time.Time)
			require.IsType(t, time.Time{}, decoded)
			got := decoded.(time.Time)
			// сравниваем только дату
			require.Equal(t, exp.Year(), got.Year())
			require.Equal(t, exp.Month(), got.Month())
			require.Equal(t, exp.Day(), got.Day())
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "time.Time",
				NewDest: func() any { var v time.Time; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(time.Time)
					got := dest.(*time.Time)
					require.Equal(t, exp.Year(), got.Year())
					require.Equal(t, exp.Month(), got.Month())
					require.Equal(t, exp.Day(), got.Day())
				},
			},
		},
	},
	{
		Name:       TypeDateTime,
		TypeName:   TypeDateTime,
		ColumnType: "DATETIME(6)",
		Values: []ValueCase{
			{"simple", time.Date(2025, 1, 2, 3, 4, 5, 123456000, time.UTC)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(time.Time)
			require.IsType(t, time.Time{}, decoded)
			got := decoded.(time.Time)
			require.True(t, exp.Equal(got), "expected %v, got %v", exp, got)
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "time.Time",
				NewDest: func() any { var v time.Time; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(time.Time)
					got := dest.(*time.Time)
					require.True(t, exp.Equal(*got), "expected %v, got %v", exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeTimestamp,
		TypeName:   TypeTimestamp,
		ColumnType: "TIMESTAMP(6)",
		Values: []ValueCase{
			{"simple", time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(time.Time)
			require.IsType(t, time.Time{}, decoded)
			got := decoded.(time.Time)
			require.True(t, exp.Equal(got))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "time.Time",
				NewDest: func() any { var v time.Time; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(time.Time)
					got := dest.(*time.Time)
					require.True(t, exp.Equal(*got))
				},
			},
		},
	},
	//{
	//	Name:       TypeTime,
	//	TypeName:   TypeTime,
	//	ColumnType: "TIME",
	//	Values: []ValueCase{
	//		{"simple", "12:34:56"},
	//	},
	//	DecodeCmp: func(t *testing.T, expected any, decoded any) {
	//		exp := expected.(time.Duration)
	//		require.IsType(t, time.Duration(0), decoded)
	//		require.Equal(t, exp, decoded.(time.Duration))
	//	},
	//	ScanDests: []ScanDestCase{
	//		{
	//			Name:    "string",
	//			NewDest: func() any { var v string; return &v },
	//			Compare: func(t *testing.T, expected any, dest any) {
	//				exp := expected.(string)
	//				got := dest.(*string)
	//				require.Equal(t, exp, *got)
	//			},
	//		},
	//	},
	//},
	{
		Name:       TypeYear,
		TypeName:   TypeYear,
		ColumnType: "YEAR(4)",
		Values: []ValueCase{
			{"y2020", int64(2020)},
			{"y1901", int64(1901)},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(int64)
			require.IsType(t, int64(0), decoded)
			require.Equal(t, exp, decoded.(int64))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "int64",
				NewDest: func() any { var v int64; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(int64)
					got := dest.(*int64)
					require.Equal(t, exp, *got)
				},
			},
		},
	},

	// ---------- STRING TYPES ----------
	{
		Name:       TypeChar,
		TypeName:   TypeChar,
		ColumnType: "CHAR(10)",
		Values: []ValueCase{
			{"empty", ""},
			{"short", "a"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(string)
			require.IsType(t, "", decoded)
			require.Equal(t, exp, strings.TrimRight(decoded.(string), " "))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(string)
					got := strings.TrimRight(*(dest.(*string)), " ")
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeVarChar,
		TypeName:   TypeVarChar,
		ColumnType: "VARCHAR(255)",
		Values: []ValueCase{
			{"empty", ""},
			{"ascii", "hello"},
			{"utf8", "привет"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(string)
			require.IsType(t, "", decoded)
			require.Equal(t, exp, decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(string)
					got := dest.(*string)
					require.Equal(t, exp, *got)
				},
			},
		},
	},
	{
		Name:       TypeTinyText,
		TypeName:   TypeTinyText,
		ColumnType: "TINYTEXT",
		Values: []ValueCase{
			{"short", "tiny text"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},
	{
		Name:       TypeText,
		TypeName:   TypeText,
		ColumnType: "TEXT",
		Values: []ValueCase{
			{"short", "some text"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},
	{
		Name:       TypeMediumText,
		TypeName:   TypeMediumText,
		ColumnType: "MEDIUMTEXT",
		Values: []ValueCase{
			{"short", "medium text"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},
	{
		Name:       TypeLongText,
		TypeName:   TypeLongText,
		ColumnType: "LONGTEXT",
		Values: []ValueCase{
			{"short", "long text (but short here)"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},

	// ---------- BINARY / BLOB ----------
	{
		Name:       TypeBinary,
		TypeName:   TypeBinary,
		ColumnType: "BINARY(4)",
		Values: []ValueCase{
			{"bytes", []byte{0x01, 0x02, 0x03, 0x04}},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeVarBinary,
		TypeName:   TypeVarBinary,
		ColumnType: "VARBINARY(8)",
		Values: []ValueCase{
			{"bytes", []byte{0x0A, 0x0B, 0x0C}},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeTinyBlob,
		TypeName:   TypeTinyBlob,
		ColumnType: "TINYBLOB",
		Values: []ValueCase{
			{"bytes", []byte("tiny blob")},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeBlob,
		TypeName:   TypeBlob,
		ColumnType: "BLOB",
		Values: []ValueCase{
			{"bytes", []byte("blob data")},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeMediumBlob,
		TypeName:   TypeMediumBlob,
		ColumnType: "MEDIUMBLOB",
		Values: []ValueCase{
			{"bytes", []byte("medium blob")},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},
	{
		Name:       TypeLongBlob,
		TypeName:   TypeLongBlob,
		ColumnType: "LONGBLOB",
		Values: []ValueCase{
			{"bytes", []byte("long blob (short in test)")},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.([]byte)
			require.IsType(t, []byte{}, decoded)
			require.Equal(t, exp, decoded.([]byte))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "[]byte",
				NewDest: func() any { var v []byte; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.([]byte)
					got := *(dest.(*[]byte))
					require.Equal(t, exp, got)
				},
			},
		},
	},

	// ---------- ENUM / SET ----------
	{
		Name:       TypeEnum,
		TypeName:   TypeEnum,
		ColumnType: "ENUM('a','b','c')",
		Values: []ValueCase{
			{"a", "a"},
			{"c", "c"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},
	{
		Name:       TypeSet,
		TypeName:   TypeSet,
		ColumnType: "SET('a','b','c')",
		Values: []ValueCase{
			{"one", "a"},
			{"multi", "a,b"},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, "", decoded)
			require.Equal(t, expected.(string), decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*string)
					require.Equal(t, expected.(string), *got)
				},
			},
		},
	},

	// ---------- BOOLEAN / BIT ----------
	{
		Name:       TypeBoolean,
		TypeName:   TypeBoolean,
		ColumnType: "BOOLEAN",
		Values: []ValueCase{
			{"false", false},
			{"true", true},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, false, decoded)
			require.Equal(t, expected.(bool), decoded.(bool))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "bool",
				NewDest: func() any { var v bool; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*bool)
					require.Equal(t, expected.(bool), *got)
				},
			},
		},
	},
	{
		Name:       TypeBool,
		TypeName:   TypeBool,
		ColumnType: "BOOL",
		Values: []ValueCase{
			{"false", false},
			{"true", true},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			require.IsType(t, false, decoded)
			require.Equal(t, expected.(bool), decoded.(bool))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "bool",
				NewDest: func() any { var v bool; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					got := dest.(*bool)
					require.Equal(t, expected.(bool), *got)
				},
			},
		},
	},
	//{
	//	Name:       TypeBit,
	//	TypeName:   TypeBit,
	//	ColumnType: "BIT(8)",
	//	Values: []ValueCase{
	//		{"val", []byte{0b10101010}},
	//	},
	//	DecodeCmp: func(t *testing.T, expected any, decoded any) {
	//		exp := expected.([]byte)
	//		require.IsType(t, []byte{}, decoded)
	//		require.Equal(t, exp, decoded.([]byte))
	//	},
	//	ScanDests: []ScanDestCase{
	//		{
	//			Name:    "[]byte",
	//			NewDest: func() any { var v []byte; return &v },
	//			Compare: func(t *testing.T, expected any, dest any) {
	//				exp := expected.([]byte)
	//				got := *(dest.(*[]byte))
	//				require.Equal(t, exp, got)
	//			},
	//		},
	//	},
	//},

	// ---------- JSON ----------
	{
		Name:       TypeJSON,
		TypeName:   TypeJSON,
		ColumnType: "JSON",
		Values: []ValueCase{
			{"object", `{"a":1,"b":"x"}`},
			{"array", `[1,2,3]`},
		},
		DecodeCmp: func(t *testing.T, expected any, decoded any) {
			exp := expected.(string)
			require.IsType(t, "", decoded)
			require.JSONEq(t, exp, decoded.(string))
		},
		ScanDests: []ScanDestCase{
			{
				Name:    "string",
				NewDest: func() any { var v string; return &v },
				Compare: func(t *testing.T, expected any, dest any) {
					exp := expected.(string)
					got := dest.(*string)
					require.JSONEq(t, exp, *got)
				},
			},
		},
	},
}

func (s *mysqlSuite) TestRestorer_EncodeValueByTypeName() {

	ctx := context.Background()
	db, err := s.GetConnection(ctx)
	s.Require().NoError(err)
	driver := New()
	s.Require().NotNil(driver)

	db, err = s.GetConnection(ctx)
	s.Require().NoError(err)

	for _, tc := range allTypeCases {
		tc := tc
		s.Run(tc.Name, func() {
			tableName, cleanup := s.createTempTable(ctx, tc, db)
			defer cleanup()

			for _, vc := range tc.Values {
				vc := vc
				s.Run(vc.Name, func() {
					encoded, err := driver.EncodeValueByTypeName(tc.TypeName, vc.Input, nil)
					s.Require().NoError(err, "encode %s/%s", tc.Name, vc.Name)

					_, err = db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE `%s`", tableName))
					s.Require().NoError(err)

					insertSQL := fmt.Sprintf(
						"INSERT INTO `%s` (data) VALUES ('%s')",
						tableName,
						string(encoded), // считаем, что encoded — уже готовый SQL-литерал
					)
					_, err = db.ExecContext(ctx, insertSQL)
					s.Require().NoError(err, "insert with encoded literal for %s/%s", tc.Name, vc.Name)
				})
			}
		})
	}
}

func (s *mysqlSuite) TestRestorer_DecodeValueByTypeName() {
	ctx := context.Background()
	driver := New()
	s.Require().NotNil(driver)

	db, err := s.GetConnection(ctx)
	s.Require().NoError(err)

	for _, tc := range allTypeCases {
		tc := tc
		s.Run(tc.Name, func() {
			tableName, cleanup := s.createTempTable(ctx, tc, db)
			defer cleanup()

			for _, vc := range tc.Values {
				vc := vc
				s.Run(vc.Name, func() {
					raw := s.insertAndFetchRaw(ctx, tableName, vc.Input)

					decoded, err := driver.DecodeValueByTypeName(tc.TypeName, raw)
					s.Require().NoError(err, "decode %s/%s", tc.Name, vc.Name)

					tc.DecodeCmp(s.T(), vc.Input, decoded)
				})
			}
		})
	}
}

func (s *mysqlSuite) TestRestorer_ScanValueByTypeName() {
	ctx := context.Background()
	driver := New()
	s.Require().NotNil(driver)

	db, err := s.GetConnection(ctx)
	s.Require().NoError(err)

	for _, tc := range allTypeCases {
		tc := tc
		s.Run(tc.Name, func() {
			tableName, cleanup := s.createTempTable(ctx, tc, db)
			defer cleanup()

			for _, vc := range tc.Values {
				vc := vc
				s.Run(vc.Name, func() {
					raw := s.insertAndFetchRaw(ctx, tableName, vc.Input)

					for _, dc := range tc.ScanDests {
						dc := dc
						s.Run(dc.Name, func() {
							dest := dc.NewDest()

							err := driver.ScanValueByTypeName(tc.TypeName, raw, dest)
							s.Require().NoError(err, "scan %s/%s -> %s", tc.Name, vc.Name, dc.Name)

							dc.Compare(s.T(), vc.Input, dest)
						})
					}
				})
			}
		})
	}
}
