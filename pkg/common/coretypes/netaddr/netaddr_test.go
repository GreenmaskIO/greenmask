// Copyright 2025 Greenmask
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package netaddr

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseInet(t *testing.T) {
	// wantIP/wantMask pin the exact *net.IPNet bytes the previous pgtype-based
	// scanIPNet produced for the same text input. Host bits must be preserved.
	tests := []struct {
		name     string
		src      string
		wantIP   string // net.IP.String()
		wantMask string // net.IPMask hex
		wantErr  bool
	}{
		{name: "ipv4 with prefix preserves host bits", src: "192.168.1.5/24", wantIP: "192.168.1.5", wantMask: "ffffff00"},
		{name: "ipv4 network", src: "192.168.1.0/24", wantIP: "192.168.1.0", wantMask: "ffffff00"},
		{name: "ipv4 no prefix defaults /32", src: "10.0.0.1", wantIP: "10.0.0.1", wantMask: "ffffffff"},
		{name: "ipv4 single host /32", src: "10.0.0.1/32", wantIP: "10.0.0.1", wantMask: "ffffffff"},
		{name: "ipv6 with prefix preserves host bits", src: "2001:db8::1/64", wantIP: "2001:db8::1", wantMask: "ffffffffffffffff0000000000000000"},
		{name: "ipv6 no prefix defaults /128", src: "2001:db8::1", wantIP: "2001:db8::1", wantMask: "ffffffffffffffffffffffffffffffff"},
		{name: "invalid garbage", src: "not-an-ip", wantErr: true},
		{name: "invalid prefix length", src: "10.0.0.1/99", wantErr: true},
		{name: "empty", src: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInet([]byte(tc.src))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantIP, got.IP.String(), "IP")
			assert.Equal(t, tc.wantMask, got.Mask.String(), "Mask")
		})
	}
}

func TestParseMacaddr(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		want    string // net.HardwareAddr.String()
		wantErr bool
	}{
		{name: "colon separated", src: "08:00:2b:01:02:03", want: "08:00:2b:01:02:03"},
		{name: "uppercase normalizes to lowercase", src: "08:00:2B:01:02:03", want: "08:00:2b:01:02:03"},
		{name: "hyphen separated", src: "08-00-2b-01-02-03", want: "08:00:2b:01:02:03"},
		{name: "eui64", src: "08:00:2b:01:02:03:04:05", want: "08:00:2b:01:02:03:04:05"},
		{name: "invalid", src: "zz:zz:zz:zz:zz:zz", wantErr: true},
		{name: "empty", src: "", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseMacaddr([]byte(tc.src))
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.String())
		})
	}
}
