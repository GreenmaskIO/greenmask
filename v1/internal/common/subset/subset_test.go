package subset

import (
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_smth(t *testing.T) {
	fset := token.NewFileSet()
	res, err := parser.ParseFile(fset, "subset.go", nil, parser.SkipObjectResolution)
	require.NoError(t, err)
	print(res.Name)
	f, err := os.Create("_some.go")
	require.NoError(t, err)
	defer f.Close()
	err = format.Node(f, fset, res)
	require.NoError(t, err)
}
