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

package storages

import (
	"context"
	"fmt"
	"path"

	commonmodels "github.com/greenmaskio/greenmask/v1/internal/common/models"
)

func walk(ctx context.Context, st Storager, parent string) ([]string, error) {
	var files []string
	var res []string
	files, dirs, err := st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing directory: %w", err)
	}
	for _, f := range files {
		res = append(res, path.Join(parent, f))
	}
	if len(dirs) > 0 {
		for _, d := range dirs {
			subFiles, err := walk(ctx, d, d.Dirname())
			if err != nil {
				return nil, fmt.Errorf("error walking through directory: %w", err)
			}
			for _, f := range subFiles {
				res = append(res, path.Join(parent, f))
			}
		}
	}
	return res, nil
}

func SubStorageWithDumpID(st Storager, dumpID commonmodels.DumpID) Storager {
	return st.SubStorage(string(dumpID), true)
}
