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

	"github.com/greenmaskio/greenmask/internal/generators"
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
	// Drop host bits so that e.g. 192.168.1.200/24 generates inside 192.168.1.0/24
	network := subnet.IP.Mask(subnet.Mask)
	if network == nil {
		return nil, fmt.Errorf("invalid subnet")
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
	if network.To4() != nil {
		hostBytes = hostBytes[:4] // Use only the first 4 bytes for IPv4
	}
	// IPv6, use all 16 bytes
	randomHostNum.SetBytes(hostBytes)
	randomHostNum.Mod(randomHostNum, new(big.Int).Sub(totalHosts, big.NewInt(2))) // [0, totalHosts-3]
	randomHostNum.Add(randomHostNum, big.NewInt(1))                               // [1, totalHosts-2]

	// Calculate the IP address
	networkInt := big.NewInt(0)
	networkInt.SetBytes(network)
	networkInt.Add(networkInt, randomHostNum)

	// FillBytes keeps leading zero bytes (0.0.0.0/8, IPv4-mapped IPv6) that Bytes drops
	res := make(net.IP, len(network))
	networkInt.FillBytes(res)
	return res, nil
}

func (ip *IpAddress) SetGenerator(g generators.Generator) error {
	if g.Size() < ip.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", ip.byteLength, g.Size())
	}
	ip.generator = g
	return nil
}
