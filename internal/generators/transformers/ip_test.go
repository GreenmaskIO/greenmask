package transformers

import (
	"net"
	"testing"
	"time"

	"github.com/greenmaskio/greenmask/internal/generators"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestIpAddress_Generate(t *testing.T) {
	type test struct {
		name    string
		subnet  string
		dynamic bool
	}

	tests := []test{
		{
			name:    "static v4",
			subnet:  "192.168.1.0/24",
			dynamic: false,
		},
		{
			name:    "dynamic v4",
			subnet:  "192.168.1.0/24",
			dynamic: true,
		},
		{
			name:    "static v6",
			subnet:  "2001:0db8:85a3::/64",
			dynamic: false,
		},
		{
			name:    "dynamic v6",
			subnet:  "2001:0db8:85a3::/64",
			dynamic: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, subnet, err := net.ParseCIDR(tt.subnet)
			require.NoError(t, err)
			tr, err := NewIpAddress(subnet)
			require.NoError(t, err)
			g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
			require.NoError(t, err)
			err = tr.SetGenerator(g)

			var res net.IP
			if tt.dynamic {
				res, err = tr.Generate([]byte{}, subnet)
			} else {
				res, err = tr.Generate([]byte{}, nil)
			}
			require.NoError(t, err)
			log.Debug().
				Str("IP", res.String()).
				Str("subnet", subnet.String()).
				Msg("results")
			require.True(t, subnet.Contains(res))
		})
	}
}

func TestIpAddress_Generate_check_address_is_not_subnet_and_broadcast(t *testing.T) {

	broadcast := net.ParseIP("192.168.1.3")
	_, subnet, err := net.ParseCIDR("192.168.1.0/30")
	require.NoError(t, err)
	tr, err := NewIpAddress(subnet)

	for i := 0; i < 100000; i++ {
		require.NoError(t, err)
		g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
		require.NoError(t, err)
		err = tr.SetGenerator(g)
		res, err := tr.Generate([]byte{}, nil)
		require.NoError(t, err)
		require.True(t, !res.Equal(broadcast) && !res.Equal(subnet.IP), "IP address is subnet or broadcast")
	}
}

func BenchmarkIpAddress_Generate(b *testing.B) {
	broadcast := net.ParseIP("192.168.1.3")
	_, subnet, err := net.ParseCIDR("192.168.1.0/30")
	require.NoError(b, err)
	tr, err := NewIpAddress(subnet)
	require.NoError(b, err)
	g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())

	for i := 0; i < b.N; i++ {
		require.NoError(b, err)
		require.NoError(b, err)
		err = tr.SetGenerator(g)
		res, err := tr.Generate([]byte{}, nil)
		require.NoError(b, err)
		require.True(b, !res.Equal(broadcast) && !res.Equal(subnet.IP), "IP address is subnet or broadcast")
	}
}
