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

func (b *IpAddress) GetRequiredGeneratorByteLength() int {
	return b.byteLength
}

func (b *IpAddress) Generate(original []byte, runtimeSubnet *net.IPNet) (net.IP, error) {

	subnet := b.subnet
	if runtimeSubnet != nil {
		subnet = runtimeSubnet
	}

	randomBytes, err := b.generator.Generate(original)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes: %w", err)
	}

	randomIP, err := randomIPInSubnet(subnet, randomBytes)
	if err != nil {
		return nil, fmt.Errorf("error generating random IP: %w", err)
	}
	return randomIP, err
}

func randomIPInSubnet(subnet *net.IPNet, randomBytes []byte) (net.IP, error) {

	netIP := subnet.IP
	mask := subnet.Mask
	hostIP := make([]byte, len(netIP))

	for i := 0; i < len(hostIP); i++ {

		val := randomBytes[i] & ^mask[i]

		hostIP[i] = val
	}

	for i := range netIP {
		netIP[i] |= hostIP[i]
	}

	return netIP, nil
}

func (b *IpAddress) SetGenerator(g generators.Generator) error {
	if g.Size() < b.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", b.byteLength, g.Size())
	}
	b.generator = g
	return nil
}
