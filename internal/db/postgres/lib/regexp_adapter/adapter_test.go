package regexp_adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_AdaptRegexp(t *testing.T) {
	testStr := `asda"As"?as*a"Te""sT"`
	expectedStr := `^(asdaAs.as.*aTe"sT)$`
	res, err := AdaptRegexp(testStr)
	assert.NoError(t, err)
	assert.Equal(t, expectedStr, res)
}
