// Copyright 2023 Greenmask
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

package filestore

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog/log"

	"github.com/greenmaskio/greenmask/internal/domains"
	"github.com/greenmaskio/greenmask/internal/storages"
	"github.com/greenmaskio/greenmask/internal/utils/ioutils"
)

type restoreSettings struct {
	cfg          *domains.FilestoreRestore
	targetPath   string
	subdir       string
	metadataName string
	cleanTarget  bool
	skipExisting bool
	usePgzip     *bool
}

// Restore downloads filestore archives from storage and extracts them into target path.
func Restore(ctx context.Context, cfg *domains.FilestoreRestore, st storages.Storager) error {
	if cfg == nil || !cfg.Enabled {
		return nil
	}
	settings, err := buildRestoreSettings(cfg)
	if err != nil {
		return err
	}
	filestoreStorage := st.SubStorage(settings.subdir, true)

	metaReader, err := filestoreStorage.GetObject(ctx, settings.metadataName)
	if err != nil {
		return fmt.Errorf("read filestore metadata: %w", err)
	}
	defer metaReader.Close()

	var meta metadata
	if err := json.NewDecoder(metaReader).Decode(&meta); err != nil {
		return fmt.Errorf("decode filestore metadata: %w", err)
	}

	if settings.usePgzip != nil {
		meta.UsePgzip = *settings.usePgzip
	}

	if settings.cleanTarget {
		if err := os.RemoveAll(settings.targetPath); err != nil {
			return fmt.Errorf("clean filestore target: %w", err)
		}
	}
	if err := os.MkdirAll(settings.targetPath, 0o750); err != nil {
		return fmt.Errorf("create filestore target: %w", err)
	}

	if len(meta.Archives) == 0 {
		log.Info().Msg("filestore restore skipped because metadata contains no archives")
		return nil
	}

	var restoredFiles int
	for _, archive := range meta.Archives {
		files, err := extractArchive(ctx, filestoreStorage, settings.targetPath, archive.Name, meta.UsePgzip, settings.skipExisting)
		if err != nil {
			return err
		}
		restoredFiles += files
	}

	log.Info().
		Int("archives", len(meta.Archives)).
		Int("files", restoredFiles).
		Str("target", settings.targetPath).
		Msg("filestore restore completed")
	return nil
}

func buildRestoreSettings(cfg *domains.FilestoreRestore) (*restoreSettings, error) {
	if cfg.TargetPath == "" {
		return nil, errors.New("restore.filestore.target_path cannot be empty")
	}
	targetPath := filepath.Clean(cfg.TargetPath)
	subdir := cfg.Subdir
	if subdir == "" {
		subdir = defaultFilestoreSubdir
	}
	metadataName := cfg.MetadataName
	if metadataName == "" {
		metadataName = defaultMetadataFileName
	}
	return &restoreSettings{
		cfg:          cfg,
		targetPath:   targetPath,
		subdir:       filepath.Clean(subdir),
		metadataName: metadataName,
		cleanTarget:  cfg.CleanTarget,
		skipExisting: cfg.SkipExisting,
		usePgzip:     cfg.UsePgzip,
	}, nil
}

func extractArchive(
	ctx context.Context,
	st storages.Storager,
	targetDir string,
	name string,
	usePgzip bool,
	skipExisting bool,
) (int, error) {
	reader, err := st.GetObject(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("download filestore archive %s: %w", name, err)
	}
	defer reader.Close()

	gzipReader, err := ioutils.GetGzipReadCloser(reader, usePgzip)
	if err != nil {
		return 0, fmt.Errorf("open gzip reader for %s: %w", name, err)
	}
	defer gzipReader.Close()

	tr := tar.NewReader(gzipReader)
	var files int
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return files, fmt.Errorf("read tar %s: %w", name, err)
		}
		if err := restoreEntry(targetDir, hdr, tr, skipExisting); err != nil {
			return files, fmt.Errorf("restore entry %s: %w", hdr.Name, err)
		}
		files++
	}
	return files, nil
}

func restoreEntry(targetDir string, hdr *tar.Header, content io.Reader, skipExisting bool) error {
	cleanName := filepath.Clean(hdr.Name)
	if strings.Contains(cleanName, "..") {
		return fmt.Errorf("unsafe path %s", hdr.Name)
	}
	fullPath := filepath.Join(targetDir, cleanName)

	switch hdr.Typeflag {
	case tar.TypeReg:
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o750); err != nil {
			return fmt.Errorf("create parent dirs: %w", err)
		}
		if skipExisting {
			if _, err := os.Stat(fullPath); err == nil {
				return nil
			}
		}
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
		if err != nil {
			return fmt.Errorf("create file: %w", err)
		}
		if _, err := io.Copy(file, content); err != nil {
			file.Close()
			return fmt.Errorf("write file: %w", err)
		}
		return file.Close()
	case tar.TypeSymlink:
		if skipExisting {
			if _, err := os.Lstat(fullPath); err == nil {
				return nil
			}
		}
		if err := os.MkdirAll(filepath.Dir(fullPath), 0o750); err != nil {
			return fmt.Errorf("create parent dirs: %w", err)
		}
		return os.Symlink(hdr.Linkname, fullPath)
	case tar.TypeDir:
		return os.MkdirAll(fullPath, os.FileMode(hdr.Mode))
	default:
		log.Warn().Str("path", hdr.Name).Msg("skipping unsupported entry type during restore")
		return nil
	}
}
