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
