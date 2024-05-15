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
	MacAddressStr  string
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

func (macAddr *MacAddress) Generate(original []byte, keepOriginalVendor bool, castType int, managementType int) ([]byte, error) {
	hostBytes, err := macAddr.generator.Generate(original)
	if err != nil {
		return nil, fmt.Errorf("error generating random bytes: %w", err)
	}

	result, err := ModifyMacAddress(hostBytes, original, keepOriginalVendor, castType, managementType)
	if err != nil {
		return nil, fmt.Errorf("can't modify mac address: %w", err)
	}

	return result, nil
}

func ModifyMacAddress(newMac, original []byte, keepOriginalVendor bool, castType, managementType int) ([]byte, error) {
	if keepOriginalVendor {
		originalMacAddrInfo, err := ParseMacAddr(original)
		if err != nil {
			return nil, fmt.Errorf("can't get original mac address info: %v", err)
		}

		// Logic with control U/L bits
		if originalMacAddrInfo.ManagementType == ManagementTypeLocal {
			newMac[0] |= 0x02
		} else {
			newMac[0] &^= 0x02
		}

		// Logic with control I/G bits
		if originalMacAddrInfo.CastType == CastTypeGroup {
			newMac[0] |= 0x01
		} else {
			newMac[0] &^= 0x01
		}

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

func ParseMacAddr(macAddress []byte) (*MacAddressInfo, error) {
	firstByte := macAddress[0]
	managementType := ManagementTypeUniversal
	if firstByte&0x02 == 0x02 {
		managementType = ManagementTypeLocal
	}

	castType := CastTypeIndividual
	if firstByte&0x01 == 0x01 {
		castType = CastTypeGroup
	}

	macSrt, err := MacBytesToString(macAddress)
	if err != nil {
		return nil, fmt.Errorf("can't create mac address string: %v", err)
	}

	return &MacAddressInfo{ManagementType: managementType, CastType: castType, MacAddressStr: macSrt}, nil
}

func (macAddr *MacAddress) SetGenerator(g generators.Generator) error {
	if g.Size() < macAddr.byteLength {
		return fmt.Errorf("requested byte length (%d) higher than generator can produce (%d)", macAddr.byteLength, g.Size())
	}
	macAddr.generator = g
	return nil
}

func MacBytesToString(originalBytes []byte) (macString string, err error) {
	if len(originalBytes) < 6 {
		return "", fmt.Errorf("incorrect size of MAC-address")
	}

	return fmt.Sprintf("%02x:%02x:%02x:%02x:%02x:%02x", originalBytes[0], originalBytes[1], originalBytes[2], originalBytes[3], originalBytes[4], originalBytes[5]), nil
}
