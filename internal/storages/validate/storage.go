package validate

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	commonininterfaces "github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/storages/domains"
)

type memoryObject struct {
	data         []byte
	lastModified time.Time
	name         string
}

type Storage struct {
	mu       sync.RWMutex
	basePath string
	files    map[string]*memoryObject
}

func (s *Storage) Ping(context.Context) error {
	return nil
}

// New initializes a root in-memory storage.
func New(basePath string) *Storage {
	return &Storage{
		basePath: basePath,
		files:    make(map[string]*memoryObject),
	}
}

func (s *Storage) GetCwd() string {
	return s.basePath
}

func (s *Storage) Dirname() string {
	return path.Base(s.basePath)
}

func (s *Storage) ListDir(_ context.Context) (files []string, dirs []commonininterfaces.Storager, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dirSet := make(map[string]bool)

	for fp := range s.files {
		rel := strings.TrimPrefix(fp, s.basePath)
		rel = strings.TrimPrefix(rel, "/")
		parts := strings.SplitN(rel, "/", 2)

		if len(parts) == 1 {
			files = append(files, parts[0])
		} else {
			dir := parts[0]
			if !dirSet[dir] {
				dirSet[dir] = true
				dirs = append(dirs, s.SubStorage(dir, true))
			}
		}
	}
	return files, dirs, nil
}

func (s *Storage) GetObject(_ context.Context, filePath string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.files[filePath]
	if !ok {
		return nil, errors.New("file not found")
	}
	return io.NopCloser(bytes.NewReader(obj.data)), nil
}

func (s *Storage) PutObject(_ context.Context, filePath string, body io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.files[filePath] = &memoryObject{
		name:         filePath,
		data:         data,
		lastModified: time.Now(),
	}
	return nil
}

func (s *Storage) Delete(_ context.Context, filePaths ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, filePath := range filePaths {
		delete(s.files, filePath)
	}
	return nil
}

func (s *Storage) DeleteAll(_ context.Context, pathPrefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for k := range s.files {
		if strings.HasPrefix(k, pathPrefix) {
			delete(s.files, k)
		}
	}
	return nil
}

func (s *Storage) Exists(_ context.Context, fileName string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.files[fileName]
	return ok, nil
}

func (s *Storage) SubStorage(subPath string, relative bool) commonininterfaces.Storager {
	newBase := subPath
	if relative {
		newBase = path.Join(s.basePath, subPath)
	}
	return &Storage{
		basePath: newBase,
		files:    s.files,
	}
}

func (s *Storage) Stat(fileName string) (*domains.ObjectStat, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.files[fileName]
	if !ok {
		return nil, errors.New("file not found")
	}

	return &domains.ObjectStat{
		Name:         path.Base(fileName),
		Exist:        true,
		LastModified: obj.lastModified,
	}, nil
}
