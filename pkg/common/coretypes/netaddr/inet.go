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

// Package netaddr provides engine-agnostic, pgx-free text codecs for the
// inet/cidr and macaddr value families. These types do not exist as native
// types in most DBMS engines, so the transformer layer owns their parsing
// rather than routing it through a per-engine driver.
//
// The inet/cidr and macaddr text parsing logic is ported from the jackc pgx
// v5 "pgtype" package (MIT licensed) to preserve byte-for-byte compatibility
// with PostgreSQL's text representation, in particular host-bit preservation
// and the default /32 and /128 masks. It is reduced to the text-format paths
// and stdlib net types that greenmask's transformers use.
package netaddr

import (
	"bytes"
	"net"
	"net/netip"
)

// ParseInet parses the PostgreSQL text representation of an inet/cidr value
// (e.g. "192.168.1.5/24", "10.0.0.1", "2001:db8::1/64") into a *net.IPNet.
//
// Host bits are preserved: "192.168.1.5/24" yields IP 192.168.1.5 with a /24
// mask (unlike net.ParseCIDR, which masks the host bits off). When the text
// omits a prefix length, the full address width is used (/32 for IPv4, /128
// for IPv6), matching PostgreSQL/pgtype semantics.
func ParseInet(src []byte) (*net.IPNet, error) {
	prefix, err := parsePrefix(src)
	if err != nil {
		return nil, err
	}
	return &net.IPNet{
		IP:   prefix.Addr().AsSlice(),
		Mask: net.CIDRMask(prefix.Bits(), prefix.Addr().BitLen()),
	}, nil
}

// parsePrefix mirrors pgtype's scanPlanTextAnyToNetipPrefixScanner: when the
// text carries no '/', the address is taken at its full bit length; otherwise
// the prefix is parsed verbatim without masking host bits.
func parsePrefix(src []byte) (netip.Prefix, error) {
	if bytes.IndexByte(src, '/') == -1 {
		addr, err := netip.ParseAddr(string(src))
		if err != nil {
			return netip.Prefix{}, err
		}
		return netip.PrefixFrom(addr, addr.BitLen()), nil
	}
	return netip.ParsePrefix(string(src))
}
