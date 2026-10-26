package transformers

import (
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"github.com/greenmaskio/greenmask/internal/generators"
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
			require.NoError(t, err)
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
	err = tr.SetGenerator(g)
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		res, err := tr.Generate([]byte{}, nil)
		require.NoError(b, err)
		require.True(b, !res.Equal(broadcast) && !res.Equal(subnet.IP), "IP address is subnet or broadcast")
	}
}

func TestIpAddress_Generate_hash_regression(t *testing.T) {
	tests := []struct {
		subnet   string
		expected string
	}{
		// Pinned outputs for regular subnets; they must not change
		{subnet: "10.0.0.0/8", expected: "10.83.252.87"},
		{subnet: "192.168.0.0/16", expected: "192.168.200.51"},
		{subnet: "192.168.1.0/24", expected: "192.168.1.11"},
		{subnet: "2001:db8:85a3::/64", expected: "2001:db8:85a3:0:3735:fef:3640:d747"},
		// Leading zero bytes used to be dropped
		{subnet: "0.0.0.0/8", expected: "0.83.252.87"},
		{subnet: "::ffff:10.0.0.0/104", expected: "10.83.252.87"},
	}
	for _, tt := range tests {
		t.Run(tt.subnet, func(t *testing.T) {
			_, subnet, err := net.ParseCIDR(tt.subnet)
			require.NoError(t, err)
			tr, err := NewIpAddress(subnet)
			require.NoError(t, err)
			g, err := generators.NewHash([]byte("salt"), "sha1")
			require.NoError(t, err)
			require.NoError(t, tr.SetGenerator(g))
			res, err := tr.Generate([]byte("a"), nil)
			require.NoError(t, err)
			require.Equal(t, tt.expected, res.String())
			require.True(t, subnet.Contains(res))
		})
	}
}

func TestIpAddress_Generate_subnet_with_host_bits(t *testing.T) {
	// Not produced by net.ParseCIDR, but possible via parameter decoding
	subnet := &net.IPNet{IP: net.ParseIP("192.168.1.200").To4(), Mask: net.CIDRMask(24, 32)}
	tr, err := NewIpAddress(subnet)
	require.NoError(t, err)
	g := generators.NewRandomBytes(time.Now().UnixNano(), tr.GetRequiredGeneratorByteLength())
	require.NoError(t, tr.SetGenerator(g))
	for i := 0; i < 1000; i++ {
		res, err := tr.Generate([]byte{}, nil)
		require.NoError(t, err)
		require.True(t, subnet.Contains(res), res.String())
	}
}
