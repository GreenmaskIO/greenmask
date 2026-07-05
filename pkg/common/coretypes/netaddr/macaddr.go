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

import "net"

// ParseMacaddr parses the text representation of a MAC address (e.g.
// "08:00:2b:01:02:03") into a net.HardwareAddr. It mirrors pgtype's text
// macaddr scan, which delegates to net.ParseMAC, so both EUI-48 (macaddr) and
// EUI-64 (macaddr8) forms are accepted.
func ParseMacaddr(src []byte) (net.HardwareAddr, error) {
	return net.ParseMAC(string(src))
}
