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

package coretest

import (
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
)

var _ core.DBMSDriver = (*Driver)(nil)

// Driver is the canonical engine-neutral core.DBMSDriver for transformer unit
// tests. Its type vocabulary is anchored on core.TypeClass, not on any vendor
// OID space, so every engine can reuse this harness unchanged. Its codecs use
// only the standard library plus the shared decimal/uuid helpers, producing the
// canonical wire formats every engine maps onto.
type Driver struct{}

// New returns a ready-to-use canonical test driver. The catalogue is package
// global and immutable, so the zero value is fully functional.
func New() *Driver {
	return &Driver{}
}

func (d *Driver) entryByName(name string) (*typeEntry, error) {
	e, ok := byName[name]
	if !ok {
		return nil, fmt.Errorf("unsupported type %q", name)
	}
	return e, nil
}

func (d *Driver) entryByID(id core.TypeID) (*typeEntry, error) {
	e, ok := byID[id]
	if !ok {
		return nil, fmt.Errorf("unsupported type id %d", id)
	}
	return e, nil
}

func (d *Driver) EncodeValueByTypeName(name string, src any, buf []byte) ([]byte, error) {
	e, err := d.entryByName(name)
	if err != nil {
		return nil, err
	}
	return e.encode(src, buf)
}

func (d *Driver) EncodeValueByTypeID(id core.TypeID, src any, buf []byte) ([]byte, error) {
	e, err := d.entryByID(id)
	if err != nil {
		return nil, err
	}
	return e.encode(src, buf)
}

func (d *Driver) DecodeValueByTypeName(name string, src []byte) (any, error) {
	e, err := d.entryByName(name)
	if err != nil {
		return nil, err
	}
	return e.decode(src)
}

func (d *Driver) DecodeValueByTypeID(id core.TypeID, src []byte) (any, error) {
	e, err := d.entryByID(id)
	if err != nil {
		return nil, err
	}
	return e.decode(src)
}

func (d *Driver) ScanValueByTypeName(name string, src []byte, dest any) error {
	e, err := d.entryByName(name)
	if err != nil {
		return err
	}
	return e.scan(src, dest)
}

func (d *Driver) ScanValueByTypeID(id core.TypeID, src []byte, dest any) error {
	e, err := d.entryByID(id)
	if err != nil {
		return err
	}
	return e.scan(src, dest)
}

func (d *Driver) TypeExistsByName(name string) bool {
	_, ok := byName[name]
	return ok
}

func (d *Driver) TypeExistsByID(id core.TypeID) bool {
	_, ok := byID[id]
	return ok
}

func (d *Driver) GetTypeID(name string) (core.TypeID, error) {
	e, err := d.entryByName(name)
	if err != nil {
		return 0, err
	}
	return e.id, nil
}

// GetCanonicalTypeName returns the canonical name of a type. The catalogue holds
// no aliases, so the name is returned as-is. The id is consulted first (matching
// real drivers that key on the stable identifier), then the name.
func (d *Driver) GetCanonicalTypeName(typeName string, typeID core.TypeID) (string, error) {
	if e, ok := byID[typeID]; ok {
		return e.name, nil
	}
	if e, ok := byName[typeName]; ok {
		return e.name, nil
	}
	return "", fmt.Errorf("find type %q: %w", typeName, core.ErrCanonicalTypeMismatch)
}

func (d *Driver) GetCanonicalTypeClassName(typeName string, typeID core.TypeID) (core.TypeClass, error) {
	if e, ok := byName[typeName]; ok {
		return e.class, nil
	}
	if e, ok := byID[typeID]; ok {
		return e.class, nil
	}
	return "", fmt.Errorf("find type class %q: %w", typeName, core.ErrUnknownDBMSTypeClass)
}
