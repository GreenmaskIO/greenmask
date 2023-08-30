package dump_config_builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildTableSearchQuery(t *testing.T) {
	var includeTable, excludeTable, excludeTableData, includeForeignData, includeSchema, excludeSchema []string
	includeTable = []string{"bookings.*"}
	excludeTable = []string{"booki*.boarding_pas*", "b?*.seats"}
	includeSchema = []string{"booki*"}
	excludeSchema = []string{"public*[[:digit:]]*1"}
	excludeTableData = []string{"bookings.flights"}
	includeForeignData = []string{"myserver"}
	res, err := BuildTableSearchQuery(includeTable, excludeTable, excludeTableData,
		includeForeignData, includeSchema, excludeSchema)
	assert.NoError(t, err)
	fmt.Println(res)
}
