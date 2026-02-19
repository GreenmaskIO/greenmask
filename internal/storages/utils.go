package storages

import (
	"context"
	"fmt"
	"path"
)

var (
	ErrFileNotFound = fmt.Errorf("file not found")
)

func Walk(ctx context.Context, st Storager, parent string) (res []string, err error) {
	var files []string
	files, dirs, err := st.ListDir(ctx)
	if err != nil {
		return nil, fmt.Errorf("error listing directory: %w", err)
	}
	for _, f := range files {
		res = append(res, path.Join(parent, f))
	}
	if len(dirs) > 0 {
		for _, d := range dirs {
			subFiles, err := Walk(ctx, d, d.Dirname())
			if err != nil {
				return nil, fmt.Errorf("error walking through directory: %w", err)
			}
			for _, f := range subFiles {
				res = append(res, path.Join(parent, f))
			}
		}
	}

	return
}
