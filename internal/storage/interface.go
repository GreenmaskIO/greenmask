package storage

import (
	"context"
	"io"
)

type Storager interface {
	Getcwd() string
	Dirname() string
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	GetReader(ctx context.Context, filePath string) (reader io.ReadCloser, err error)
	GetWriter(ctx context.Context, filePath string) (writer io.WriteCloser, err error)
	Delete(ctx context.Context, filePath string, recursive bool) error
	Chdir(ctx context.Context, dirPath string) error
	CreateDir(ctx context.Context, dirName string) (Storager, error)
	Rename(ctx context.Context, original, new string) error
	Exists(ctx context.Context, fileName string) (bool, error)
}
