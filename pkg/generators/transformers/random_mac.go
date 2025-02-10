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

	"github.com/greenmaskio/greenmask/pkg/generators"
)

const (
	CastTypeIndividual = iota
	CastTypeGroup
	CastTypeAny
)

const (
	ManagementTypeUniversal = iota
	ManagementTypeLocal
	ManagementTypeAny
)

type MacAddress struct {
	generator  generators.Generator
	byteLength int
}

type MacAddressInfo struct {
	MacAddress     net.HardwareAddr
	ManagementType int
	CastType       int
}

func NewMacAddress() (*MacAddress, error) {
	return &MacAddress{
		byteLength: 6,
	}, nil
}

func (macAddr *MacAddress) GetRequiredGeneratorByteLength() int {
	return macAddr.byteLength
}

func (macAddr *MacAddress) Generate(original net.HardwareAddr, keepOriginalVendor bool, castType int, managementType int) (net.HardwareAddr, error) {
	randoBytes, err := macAddr.generator.Generate(original)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes: %w", err)
	}
	randomMac, err := RandomBytesToHardwareAddr(randoBytes)
	if err != nil {
		return nil, fmt.Errorf("error converting random bytes to hardware address: %w", err)
	}

	result, err := ModifyMacAddress(randomMac, original, keepOriginalVendor, castType, managementType)
	if err != nil {
		return nil, fmt.Errorf("can't modify mac address: %w", err)
	}

	return result, nil
}

func ModifyMacAddress(newMac, original net.HardwareAddr, keepOriginalVendor bool, castType, managementType int) ([]byte, error) {
	if keepOriginalVendor {
		newMac[0] = original[0]
		newMac[1] = original[1]
		newMac[2] = original[2]
	} else {
		if managementType == ManagementTypeUniversal || managementType == ManagementTypeLocal {
			if managementType == ManagementTypeLocal {
				newMac[0] |= 0x02
			} else {
				newMac[0] &^= 0x02
			}
		}

		if castType == CastTypeIndividual || castType == CastTypeGroup {
			if castType == CastTypeGroup {
				newMac[0] |= 0x01
			} else {
				newMac[0] &^= 0x01
			}
		}
	}

	return newMac, nil
}

// ExploreMacAddress - explore mac address and return info about it
func ExploreMacAddress(macAddress net.HardwareAddr) (*MacAddressInfo, error) {
	firstByte := macAddress[0]
	managementType := ManagementTypeUniversal
	if firstByte&0x02 == 0x02 {
		managementType = ManagementTypeLocal
	}

	castType := CastTypeIndividual
	if firstByte&0x01 == 0x01 {
		castType = CastTypeGroup
	}

	return &MacAddressInfo{ManagementType: managementType, CastType: castType, MacAddress: macAddress}, nil
}

func (macAddr *MacAddress) SetGenerator(g generators.Generator) error {
	if g.Size() < macAddr.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", macAddr.byteLength, g.Size())
	}
	macAddr.generator = g
	return nil
}

func RandomBytesToHardwareAddr(originalBytes []byte) (net.HardwareAddr, error) {
	return net.ParseMAC(
		fmt.Sprintf(
			"%02x:%02x:%02x:%02x:%02x:%02x",
			originalBytes[0], originalBytes[1], originalBytes[2], originalBytes[3], originalBytes[4], originalBytes[5],
		),
	)
}
