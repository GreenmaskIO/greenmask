package generators

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestSibHashHybrid(t *testing.T) {
	expected := []byte{0x8b, 0x16, 0xe4, 0xaa, 0x8d, 0x9, 0x2, 0xf1, 0x82, 0xcf}
	requiredLength := 10
	sp, err := NewSipHash([]byte("test"))
	require.NoError(t, err)
	hb := NewHybridBytes(0, requiredLength, sp)
	res, err := hb.Generate([]byte("test"))
	log.Debug().
		Bytes("Res", res).
		Msg("")
	require.NoError(t, err)
	require.Equal(t, expected, res)
}
