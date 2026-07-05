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

package core

import "context"

// StorageProvisioner builds the destination Storager for a dump run from
// configuration. Provision receives the full config as any to avoid an import
// cycle (pkg/config already imports this package); implementations type-assert
// to config.Config internally. Storage backend selection (directory, S3) is
// engine-agnostic.
type StorageProvisioner interface {
	Provision(ctx context.Context, cfg any) (Storager, error)
}
