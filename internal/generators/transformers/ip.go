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
	"github.com/greenmaskio/greenmask/internal/generators"
	"math/rand"
	"net"
	"time"
)

type IpAddress struct {
	subnet     string
	generator  generators.Generator
	byteLength int
}

func NewIpAddress(subnet string) *IpAddress {
	return &IpAddress{
		byteLength: 1,
		subnet:     subnet,
	}
}

func (b *IpAddress) GetRequiredGeneratorByteLength() int {
	return b.byteLength
}

func (b *IpAddress) Generate() (string, error) {
	randomIP, err := randomIPInSubnet(b.subnet)
	if err != nil {
		fmt.Println("Ошибка:", err)
		return "", nil
	}
	return randomIP.String(), err
}

func randomIPInSubnet(cidr string) (net.IP, error) {
	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return nil, err
	}

	netIP := subnet.IP
	mask := subnet.Mask
	hostIP := make([]byte, len(netIP))

	src := rand.NewSource(time.Now().UnixNano())
	rng := rand.New(src)

	for i := 0; i < len(hostIP); i++ {
		val := byte(rng.Intn(256) & ^int(mask[i]))

		if val == 1 {
			val += 1
		}

		if val == 255 {
			val -= 1
		}

		hostIP[i] = byte(rng.Intn(256) & ^int(mask[i]))
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
