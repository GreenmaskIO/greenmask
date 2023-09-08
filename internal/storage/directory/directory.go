package directory

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"syscall"

	"github.com/greenmaskio/greenmask/internal/storage"
)

type Directory struct {
	dirMode  os.FileMode
	fileMode os.FileMode
	cwd      string
}

func NewDirectory(cwd string, dirMode, fileMode os.FileMode) (*Directory, error) {
	// TODO: We would replace hardcoded file mask to Umask for unix system
	fileInfo, err := os.Stat(cwd)
	if err != nil {
		return nil, err
	}
	if !fileInfo.IsDir() {
		return nil, errors.New("received directory path is file")
	}
	return &Directory{
		dirMode:  dirMode,
		fileMode: fileMode,
		cwd:      cwd,
	}, nil
}

func (d *Directory) Getcwd() string {
	return d.cwd
}

func (d *Directory) Dirname() string {
	return filepath.Base(d.cwd)
}

func (d *Directory) ListDir(ctx context.Context) (files []string, dirs []storage.Storager, err error) {
	entries, err := os.ReadDir(d.cwd)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, &Directory{
				cwd:      path.Join(d.cwd, entry.Name()),
				dirMode:  d.dirMode,
				fileMode: d.fileMode,
			})
		} else {
			files = append(files, entry.Name())
		}
	}
	return
}

func (d *Directory) GetReader(ctx context.Context, filePath string) (reader io.ReadCloser, err error) {
	reader, err = os.Open(path.Join(d.cwd, filePath))
	return
}

func (d *Directory) GetWriter(ctx context.Context, filePath string) (writer io.WriteCloser, err error) {
	f, err := os.Create(path.Join(d.cwd, filePath))
	if err != nil {
		return
	}
	return f, nil
}

func (d *Directory) Delete(ctx context.Context, filePath string, recursive bool) error {
	fileInfo, err := os.Stat(path.Join(d.cwd, filePath))
	if err != nil {
		return err
	}
	if fileInfo.IsDir() {
		if !recursive {
			return errors.New("attempt deleting directory in non recursive mode")
		}
		return os.RemoveAll(path.Join(d.cwd, filePath))
	}
	return os.Remove(path.Join(d.cwd, filePath))
}

func (d *Directory) Chdir(ctx context.Context, dirPath string) error {
	newPath := dirPath
	if dirPath[1] != '/' {
		newPath = path.Join(d.cwd, dirPath)
	}
	fileInfo, err := os.Stat(newPath)
	if err != nil {
		return err
	}
	if !fileInfo.IsDir() {
		return errors.New("received directory path is file")
	}
	d.cwd = newPath
	return nil
}

func (d *Directory) CreateDir(ctx context.Context, dirName string) (storage.Storager, error) {
	if err := os.Mkdir(path.Join(d.cwd, dirName), os.ModePerm); err != nil {
		return nil, err
	}
	return &Directory{
		cwd: path.Join(d.cwd, dirName),
	}, nil
}

func (d *Directory) Rename(ctx context.Context, original, new string) error {
	return os.Rename(path.Join(d.cwd, original), path.Join(d.cwd, new))
}

func (d *Directory) Exists(ctx context.Context, fileName string) (bool, error) {
	_, err := os.Stat(path.Join(d.cwd, fileName))
	if err != nil {
		if errors.Is(err, syscall.ENOENT) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}
