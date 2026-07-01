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

package storageprovisioner

import (
	"context"
	"fmt"

	core "github.com/greenmaskio/greenmask/pkg/common/core"
	"github.com/greenmaskio/greenmask/pkg/common/utils"
	"github.com/greenmaskio/greenmask/pkg/config"
)

var _ core.StorageProvisioner = (*Provisioner)(nil)

// Provisioner is the engine-agnostic core.StorageProvisioner. It selects and
// builds the storage backend (directory, S3) purely from configuration.
type Provisioner struct{}

func New() *Provisioner {
	return &Provisioner{}
}

func (p *Provisioner) Provision(ctx context.Context, cfg any) (core.Storager, error) {
	c, ok := cfg.(config.Config)
	if !ok {
		return nil, fmt.Errorf("storage provisioner: unexpected config type %T", cfg)
	}
	return utils.GetStorage(ctx, &c)
}
