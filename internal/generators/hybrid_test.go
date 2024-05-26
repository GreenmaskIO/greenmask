package generators

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestSibHashHybrid(t *testing.T) {
	expected := []byte{176, 20, 124, 157, 15, 119, 202, 213, 41, 32}
	requiredLength := 10
	sp, err := NewSipHash([]byte("test"))
	require.NoError(t, err)
	hb := NewHybridBytes(0, requiredLength, sp)
	res, err := hb.Generate([]byte("test"))
	log.Debug().
		Bytes("Res", res).
		Msg("")
	require.NoError(t, err)
	require.Equal(t, res, expected)
}
