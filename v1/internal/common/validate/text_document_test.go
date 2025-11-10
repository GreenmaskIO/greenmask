package validate

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTextDocument_GetRecords(t *testing.T) {
	t.Run("only transformed vertical with diff", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, true, true, TableFormatNameVertical)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("only transformed vertical without diff", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, false, true, TableFormatNameVertical)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("all vertical", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, true, false, TableFormatNameVertical)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("all vertical without diff", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, false, false, TableFormatNameVertical)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("only transformed horizontal", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, true, true, TableFormatNameHorizontal)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("only transformed horizontal without diff", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, false, true, TableFormatNameHorizontal)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("all horizontal", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, true, false, TableFormatNameHorizontal)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})

	t.Run("all horizontal without diff", func(t *testing.T) {
		tab, originalRecs, transformedRecs := getTableAndRows(t)
		affectedColumns := []int{3}

		jd := NewTextDocument(tab, affectedColumns, false, false, TableFormatNameHorizontal)

		for i := range originalRecs {
			err := jd.Append(originalRecs[i], transformedRecs[i])
			require.NoError(t, err)
		}

		res, err := jd.Marshall()
		require.NoError(t, err)
		print(t, string(res))
	})
}
