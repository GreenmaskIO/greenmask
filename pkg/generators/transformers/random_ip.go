// Copyright 2023 Greenmask
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

package transformers

import (
	"fmt"
	"math/big"
	"net"

	"github.com/greenmaskio/greenmask/pkg/generators"
)

type IpAddress struct {
	subnet     *net.IPNet
	generator  generators.Generator
	byteLength int
}

func NewIpAddress(subnet *net.IPNet) (*IpAddress, error) {
	return &IpAddress{
		byteLength: 16,
		subnet:     subnet,
	}, nil
}

func (ip *IpAddress) GetRequiredGeneratorByteLength() int {
	return ip.byteLength
}

func (ip *IpAddress) Generate(original []byte, runtimeSubnet *net.IPNet) (net.IP, error) {
	subnet := ip.subnet
	if runtimeSubnet != nil {
		subnet = runtimeSubnet
	}
	ones, bits := subnet.Mask.Size()
	totalHosts := big.NewInt(1)
	totalHosts.Lsh(totalHosts, uint(bits-ones))

	if totalHosts.Cmp(big.NewInt(2)) <= 0 {
		return nil, fmt.Errorf("subnet too small")
	}

	// Generate random host part within the range, avoiding special addresses
	randomHostNum := big.NewInt(0)

	hostBytes, err := ip.generator.Generate(original)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes: %w", err)
	}
	if subnet.IP.To4() != nil {
		hostBytes = hostBytes[:4] // Use only the first 4 bytes for IPv4
	}
	// IPv6, use all 16 bytes
	randomHostNum.SetBytes(hostBytes)
	randomHostNum.Mod(randomHostNum, new(big.Int).Sub(totalHosts, big.NewInt(2))) // [0, totalHosts-3]
	randomHostNum.Add(randomHostNum, big.NewInt(1))                               // [1, totalHosts-2]

	// Calculate the IP address
	networkInt := big.NewInt(0)
	networkInt.SetBytes(subnet.IP)
	networkInt.Add(networkInt, randomHostNum)

	ipAddrBytes := networkInt.Bytes()
	if len(ipAddrBytes) < 16 && subnet.IP.To4() == nil {
		// Pad the address to 16 bytes if it's an IPv6 address
		paddedIP := make([]byte, 16)
		copy(paddedIP[16-len(ipAddrBytes):], ipAddrBytes)
		return paddedIP, nil
	}
	return ipAddrBytes, nil
}

func (ip *IpAddress) SetGenerator(g generators.Generator) error {
	if g.Size() < ip.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ip.byteLength, g.Size())
	}
	ip.generator = g
	return nil
}
