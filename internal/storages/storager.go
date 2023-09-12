package storages

import (
	"context"
	"io"
)

type Storager interface {
	// GetCwd - get current working directory (CWD) path
	GetCwd() string
	// Dirname - returns current dirname without prefix
	Dirname() string
	// ListDir - walking through storage and returns directories and files in the cwd
	ListDir(ctx context.Context) (files []string, dirs []Storager, err error)
	// GetObject - returns ReadCloser by the provided path
	GetObject(ctx context.Context, filePath string) (reader io.ReadCloser, err error)
	// PutObject - puts data to the provided file path
	PutObject(ctx context.Context, filePath string, body io.Reader) error
	// DeleteV2 - delete list of objects by the provided paths
	DeleteV2(ctx context.Context, filePaths ...string) error
	// Exists - check object existence
	Exists(ctx context.Context, fileName string) (bool, error)
	// SubStorage - get new Storage instance with the samo config but change current cwd via subPath
	// If relative == true then path is sub folder in cwd
	SubStorage(subPath string, relative bool) Storager

	// GetWriter - old method for writing objects
	// Deprecated
	GetWriter(ctx context.Context, filePath string) (writer io.WriteCloser, err error)
	// Delete - delete object by path
	// Deprecated
	Delete(ctx context.Context, filePath string, recursive bool) error
	// Chdir - change cwd in Storage
	// Deprecated
	Chdir(ctx context.Context, dirPath string) error
	// CreateDir - create directory.
	// Instead of creating new directory - new directory must be created if not exist by default
	// Deprecated
	CreateDir(ctx context.Context, dirName string) (Storager, error)
	// Rename - rename existed object. It's better create two methods - Copy and Move
	// Deprecated
	Rename(ctx context.Context, original, new string) error
}
